# -*- coding: utf-8 -*-
"""
Job Xây dựng Bảng Sự kiện (Fact Job).
Job này đọc dữ liệu Staging (Silver) và các Bảng Danh mục (Gold),
tiến hành Join (Kết nối) để lấy Surrogate Keys, sau đó tính toán
các trường đo lường (Metrics) như Doanh thu, Tiền bảo hiểm trả,...
và lưu vào tầng Gold trên HDFS.
"""

import sys
from hospital_utils import create_spark, read_parquet, write_parquet, write_error


def main(run_date: str):
    """
    Hàm thực thi chính của Fact Job.
    
    Args:
        run_date (str): Ngày chạy ETL, dùng để phân vùng thư mục HDFS.
    """
    spark = create_spark("HospitalFactJob")

    silver_base = f"hdfs://hdfs-namenode:8020/hospital_etl/silver/run_date={run_date}"
    gold_base = f"hdfs://hdfs-namenode:8020/hospital_etl/gold/run_date={run_date}"
    quarantine_base = f"hdfs://hdfs-namenode:8020/hospital_etl/quarantine/run_date={run_date}"

    for name in ["stg_visits", "stg_service_transactions", "stg_prescriptions"]:
        read_parquet(spark, f"{silver_base}/{name}").createOrReplaceTempView(name)

    for name in [
        "Dim_Date", "Dim_Patient", "Dim_Doctor", "Dim_Department",
        "Dim_Service", "Dim_Medicine", "Dim_Insurance"
    ]:
        read_parquet(spark, f"{gold_base}/{name}").createOrReplaceTempView(name)

    # ---------------------------------------------------------
    # 1. Xử lý Lỗi (Data Quality / Quarantine)
    # Tìm các bản ghi Fact không khớp được với Dimension (Orphan records)
    # ---------------------------------------------------------
    fact_error_queries = {
        "fact_visit_missing_dimension": """
            SELECT v.*,
                CASE
                    WHEN d.DateKey IS NULL THEN 'missing_date_dimension'
                    WHEN p.PatientKey IS NULL THEN 'missing_patient_dimension'
                    WHEN doc.DoctorKey IS NULL THEN 'missing_doctor_dimension'
                    WHEN dep.DepartmentKey IS NULL THEN 'missing_department_dimension'
                    WHEN ins.InsuranceKey IS NULL THEN 'missing_insurance_dimension'
                    ELSE 'unknown_missing_dimension'
                END AS error_reason
            FROM stg_visits v
            LEFT JOIN Dim_Date d ON v.VisitDate = d.FullDate
            LEFT JOIN Dim_Patient p ON v.PatientCode = p.PatientCode
            LEFT JOIN Dim_Doctor doc ON v.DoctorCode = doc.DoctorCode
            LEFT JOIN Dim_Department dep ON v.DepartmentCode = dep.DepartmentCode
            LEFT JOIN Dim_Insurance ins ON v.InsuranceCode = ins.InsuranceCode
            WHERE d.DateKey IS NULL
               OR p.PatientKey IS NULL
               OR doc.DoctorKey IS NULL
               OR dep.DepartmentKey IS NULL
               OR ins.InsuranceKey IS NULL
        """,
        "fact_service_revenue_missing_dimension": """
            SELECT s.*,
                CASE
                    WHEN d.DateKey IS NULL THEN 'missing_date_dimension'
                    WHEN p.PatientKey IS NULL THEN 'missing_patient_dimension'
                    WHEN doc.DoctorKey IS NULL THEN 'missing_doctor_dimension'
                    WHEN dep.DepartmentKey IS NULL THEN 'missing_department_dimension'
                    WHEN svc.ServiceKey IS NULL THEN 'missing_service_dimension'
                    WHEN ins.InsuranceKey IS NULL THEN 'missing_insurance_dimension'
                    ELSE 'unknown_missing_dimension'
                END AS error_reason
            FROM stg_service_transactions s
            LEFT JOIN Dim_Date d ON s.ServiceDate = d.FullDate
            LEFT JOIN Dim_Patient p ON s.PatientCode = p.PatientCode
            LEFT JOIN Dim_Doctor doc ON s.DoctorCode = doc.DoctorCode
            LEFT JOIN Dim_Department dep ON s.DepartmentCode = dep.DepartmentCode
            LEFT JOIN Dim_Service svc ON s.ServiceCode = svc.ServiceCode
            LEFT JOIN Dim_Insurance ins ON s.InsuranceCode = ins.InsuranceCode
            WHERE d.DateKey IS NULL
               OR p.PatientKey IS NULL
               OR doc.DoctorKey IS NULL
               OR dep.DepartmentKey IS NULL
               OR svc.ServiceKey IS NULL
               OR ins.InsuranceKey IS NULL
        """,
        "fact_prescription_missing_dimension": """
            SELECT pr.*,
                CASE
                    WHEN d.DateKey IS NULL THEN 'missing_date_dimension'
                    WHEN p.PatientKey IS NULL THEN 'missing_patient_dimension'
                    WHEN doc.DoctorKey IS NULL THEN 'missing_doctor_dimension'
                    WHEN dep.DepartmentKey IS NULL THEN 'missing_department_dimension'
                    WHEN med.MedicineKey IS NULL THEN 'missing_medicine_dimension'
                    WHEN ins.InsuranceKey IS NULL THEN 'missing_insurance_dimension'
                    ELSE 'unknown_missing_dimension'
                END AS error_reason
            FROM stg_prescriptions pr
            LEFT JOIN Dim_Date d ON pr.PrescriptionDate = d.FullDate
            LEFT JOIN Dim_Patient p ON pr.PatientCode = p.PatientCode
            LEFT JOIN Dim_Doctor doc ON pr.DoctorCode = doc.DoctorCode
            LEFT JOIN Dim_Department dep ON pr.DepartmentCode = dep.DepartmentCode
            LEFT JOIN Dim_Medicine med ON pr.MedicineCode = med.MedicineCode
            LEFT JOIN Dim_Insurance ins ON pr.InsuranceCode = ins.InsuranceCode
            WHERE d.DateKey IS NULL
               OR p.PatientKey IS NULL
               OR doc.DoctorKey IS NULL
               OR dep.DepartmentKey IS NULL
               OR med.MedicineKey IS NULL
               OR ins.InsuranceKey IS NULL
        """
    }

    for folder, query in fact_error_queries.items():
        write_error(spark.sql(query), f"{quarantine_base}/{folder}")

    # ---------------------------------------------------------
    # 2. Tạo Bảng Fact_Visit (Lượt khám)
    # Join với các Dimension để lấy Key. Tính toán VisitCount = 1
    # ---------------------------------------------------------
    fact_visit = spark.sql("""
        WITH valid AS (
            SELECT
                d.DateKey,
                p.PatientKey,
                doc.DoctorKey,
                dep.DepartmentKey,
                ins.InsuranceKey,
                v.VisitCode,
                v.ConsultationFee,
                v.WaitingMinutes,
                1 AS VisitCount,
                ROW_NUMBER() OVER (
                    PARTITION BY v.VisitCode
                    ORDER BY v.updated_at DESC, v.visit_id DESC
                ) AS rn
            FROM stg_visits v
            JOIN Dim_Date d ON v.VisitDate = d.FullDate
            JOIN Dim_Patient p ON v.PatientCode = p.PatientCode
            JOIN Dim_Doctor doc ON v.DoctorCode = doc.DoctorCode
            JOIN Dim_Department dep ON v.DepartmentCode = dep.DepartmentCode
            JOIN Dim_Insurance ins ON v.InsuranceCode = ins.InsuranceCode
        )
        SELECT
            monotonically_increasing_id() AS VisitFactKey,
            DateKey,
            PatientKey,
            DoctorKey,
            DepartmentKey,
            InsuranceKey,
            VisitCode,
            ConsultationFee,
            WaitingMinutes,
            VisitCount
        FROM valid
        WHERE rn = 1
    """)

    # ---------------------------------------------------------
    # 3. Tạo Bảng Fact_ServiceRevenue (Doanh thu Dịch vụ)
    # Tính toán chi phí Bệnh nhân trả và Bảo hiểm trả dựa trên CoverageRate
    # ---------------------------------------------------------
    fact_service_revenue = spark.sql("""
        WITH valid AS (
            SELECT
                d.DateKey,
                p.PatientKey,
                doc.DoctorKey,
                dep.DepartmentKey,
                svc.ServiceKey,
                ins.InsuranceKey,
                s.VisitCode,
                s.ServiceTransactionCode,
                s.Quantity,
                s.UnitPrice,
                s.RevenueAmount,
                CAST(s.RevenueAmount * ins.CoverageRate / 100 AS DECIMAL(18,2)) AS InsurancePaidAmount,
                CAST(s.RevenueAmount - (s.RevenueAmount * ins.CoverageRate / 100) AS DECIMAL(18,2)) AS PatientPaidAmount,
                ROW_NUMBER() OVER (
                    PARTITION BY s.ServiceTransactionCode
                    ORDER BY s.updated_at DESC, s.service_transaction_id DESC
                ) AS rn
            FROM stg_service_transactions s
            JOIN Dim_Date d ON s.ServiceDate = d.FullDate
            JOIN Dim_Patient p ON s.PatientCode = p.PatientCode
            JOIN Dim_Doctor doc ON s.DoctorCode = doc.DoctorCode
            JOIN Dim_Department dep ON s.DepartmentCode = dep.DepartmentCode
            JOIN Dim_Service svc ON s.ServiceCode = svc.ServiceCode
            JOIN Dim_Insurance ins ON s.InsuranceCode = ins.InsuranceCode
        )
        SELECT
            monotonically_increasing_id() AS ServiceRevenueFactKey,
            DateKey,
            PatientKey,
            DoctorKey,
            DepartmentKey,
            ServiceKey,
            InsuranceKey,
            VisitCode,
            ServiceTransactionCode,
            Quantity,
            UnitPrice,
            RevenueAmount,
            InsurancePaidAmount,
            PatientPaidAmount
        FROM valid
        WHERE rn = 1
    """)

    fact_prescription = spark.sql("""
        WITH valid AS (
            SELECT
                d.DateKey,
                p.PatientKey,
                doc.DoctorKey,
                dep.DepartmentKey,
                med.MedicineKey,
                ins.InsuranceKey,
                pr.VisitCode,
                pr.PrescriptionCode,
                pr.Quantity,
                pr.UnitPrice,
                pr.MedicineRevenueAmount,
                CAST(pr.MedicineRevenueAmount * ins.CoverageRate / 100 AS DECIMAL(18,2)) AS InsurancePaidAmount,
                CAST(pr.MedicineRevenueAmount - (pr.MedicineRevenueAmount * ins.CoverageRate / 100) AS DECIMAL(18,2)) AS PatientPaidAmount,
                ROW_NUMBER() OVER (
                    PARTITION BY pr.PrescriptionCode
                    ORDER BY pr.updated_at DESC, pr.prescription_id DESC
                ) AS rn
            FROM stg_prescriptions pr
            JOIN Dim_Date d ON pr.PrescriptionDate = d.FullDate
            JOIN Dim_Patient p ON pr.PatientCode = p.PatientCode
            JOIN Dim_Doctor doc ON pr.DoctorCode = doc.DoctorCode
            JOIN Dim_Department dep ON pr.DepartmentCode = dep.DepartmentCode
            JOIN Dim_Medicine med ON pr.MedicineCode = med.MedicineCode
            JOIN Dim_Insurance ins ON pr.InsuranceCode = ins.InsuranceCode
        )
        SELECT
            monotonically_increasing_id() AS PrescriptionFactKey,
            DateKey,
            PatientKey,
            DoctorKey,
            DepartmentKey,
            MedicineKey,
            InsuranceKey,
            VisitCode,
            PrescriptionCode,
            Quantity,
            UnitPrice,
            MedicineRevenueAmount,
            InsurancePaidAmount,
            PatientPaidAmount
        FROM valid
        WHERE rn = 1
    """)

    print("Fact counts:")
    print("Fact_Visit =", fact_visit.count())
    print("Fact_ServiceRevenue =", fact_service_revenue.count())
    print("Fact_Prescription =", fact_prescription.count())

    # ---------------------------------------------------------
    # 5. Ghi kết quả ra tầng Gold (Định dạng Parquet)
    # ---------------------------------------------------------
    write_parquet(fact_visit, f"{gold_base}/Fact_Visit")
    write_parquet(fact_service_revenue, f"{gold_base}/Fact_ServiceRevenue")
    write_parquet(fact_prescription, f"{gold_base}/Fact_Prescription")

    print("Fact job completed successfully.")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: hospital_fact_job.py <YYYY-MM-DD>")
        sys.exit(1)

    main(sys.argv[1])
