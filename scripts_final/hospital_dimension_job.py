# -*- coding: utf-8 -*-
"""
Job Xây dựng Bảng Danh mục (Dimension Job).
Job này đọc dữ liệu đã làm sạch từ tầng Silver (Staging) trên HDFS,
sau đó sinh khóa chính thay thế (Surrogate Key) phân tán,
và cuối cùng lưu vào tầng Gold trên HDFS dưới dạng Parquet.
"""

import sys
from hospital_utils import create_spark, read_parquet, write_parquet


def main(run_date: str):
    """
    Hàm thực thi chính của Dimension Job.
    
    Args:
        run_date (str): Ngày chạy ETL (định dạng YYYY-MM-DD), dùng để phân vùng thư mục HDFS.
    """
    spark = create_spark("HospitalDimensionJob")

    silver_base = f"hdfs://hdfs-namenode:8020/hospital_etl/silver/run_date={run_date}"
    gold_base = f"hdfs://hdfs-namenode:8020/hospital_etl/gold/run_date={run_date}"

    # Load tất cả các bảng từ Silver layer vào Temp Views để truy vấn bằng SQL
    for name in [
        "stg_patients", "stg_doctors", "stg_departments", "stg_services", "stg_medicines",
        "stg_insurance", "stg_visits", "stg_service_transactions", "stg_prescriptions"
    ]:
        read_parquet(spark, f"{silver_base}/{name}").createOrReplaceTempView(name)

    # ---------------------------------------------------------
    # 1. Tạo Bảng Dim_Patient
    # Sử dụng monotonically_increasing_id() để tạo Surrogate Key tốc độ cao.
    # Sử dụng ROW_NUMBER() OVER (PARTITION BY) để lấy bản ghi mới nhất của mỗi PatientCode.
    # ---------------------------------------------------------

    dim_patient = spark.sql("""
        WITH x AS (
            SELECT
                PatientCode,
                FullName,
                Gender,
                BirthYear,
                AgeGroup,
                City,
                ROW_NUMBER() OVER (
                    PARTITION BY PatientCode
                    ORDER BY updated_at DESC, patient_id DESC
                ) AS rn
            FROM stg_patients
        )
        SELECT monotonically_increasing_id() AS PatientKey,
               PatientCode, FullName, Gender, BirthYear, AgeGroup, City
        FROM x WHERE rn = 1
    """)

    dim_doctor = spark.sql("""
        WITH x AS (
            SELECT
                DoctorCode,
                DoctorName,
                Gender,
                Specialty,
                AcademicTitle,
                ROW_NUMBER() OVER (
                    PARTITION BY DoctorCode
                    ORDER BY updated_at DESC, doctor_id DESC
                ) AS rn
            FROM stg_doctors
        )
        SELECT monotonically_increasing_id() AS DoctorKey,
               DoctorCode, DoctorName, Gender, Specialty, AcademicTitle
        FROM x WHERE rn = 1
    """)

    dim_department = spark.sql("""
        WITH x AS (
            SELECT
                DepartmentCode,
                DepartmentName,
                DepartmentGroup,
                ROW_NUMBER() OVER (
                    PARTITION BY DepartmentCode
                    ORDER BY updated_at DESC, department_id DESC
                ) AS rn
            FROM stg_departments
        )
        SELECT monotonically_increasing_id() AS DepartmentKey,
               DepartmentCode, DepartmentName, DepartmentGroup
        FROM x WHERE rn = 1
    """)

    dim_service = spark.sql("""
        WITH x AS (
            SELECT
                ServiceCode,
                ServiceName,
                ServiceType,
                ROW_NUMBER() OVER (
                    PARTITION BY ServiceCode
                    ORDER BY updated_at DESC, service_id DESC
                ) AS rn
            FROM stg_services
        )
        SELECT monotonically_increasing_id() AS ServiceKey,
               ServiceCode, ServiceName, ServiceType
        FROM x WHERE rn = 1
    """)

    dim_medicine = spark.sql("""
        WITH x AS (
            SELECT
                MedicineCode,
                MedicineName,
                MedicineGroup,
                Unit,
                ROW_NUMBER() OVER (
                    PARTITION BY MedicineCode
                    ORDER BY updated_at DESC, medicine_id DESC
                ) AS rn
            FROM stg_medicines
        )
        SELECT monotonically_increasing_id() AS MedicineKey,
               MedicineCode, MedicineName, MedicineGroup, Unit
        FROM x WHERE rn = 1
    """)

    dim_insurance = spark.sql("""
        WITH x AS (
            SELECT
                InsuranceCode,
                InsuranceType,
                CoverageRate,
                ROW_NUMBER() OVER (
                    PARTITION BY InsuranceCode
                    ORDER BY updated_at DESC, insurance_id DESC
                ) AS rn
            FROM stg_insurance
        )
        SELECT monotonically_increasing_id() AS InsuranceKey,
               InsuranceCode, InsuranceType, CoverageRate
        FROM x WHERE rn = 1
    """)

    # ---------------------------------------------------------
    # 2. Tạo Bảng Dim_Date (Dimension Thời gian)
    # Gom tất cả các ngày phát sinh từ Visit, Service, Prescription
    # ---------------------------------------------------------
    dim_date = spark.sql("""
        WITH all_dates AS (
            SELECT VisitDate AS FullDate FROM stg_visits
            UNION
            SELECT ServiceDate AS FullDate FROM stg_service_transactions
            UNION
            SELECT PrescriptionDate AS FullDate FROM stg_prescriptions
        ), d AS (
            SELECT DISTINCT FullDate FROM all_dates WHERE FullDate IS NOT NULL
        )
        SELECT CAST(DATE_FORMAT(FullDate,'yyyyMMdd') AS INT) AS DateKey,
               FullDate,
               CAST(DAYOFMONTH(FullDate) AS INT) AS DayNumber,
               CAST(MONTH(FullDate) AS INT) AS MonthNumber,
               DATE_FORMAT(FullDate,'MMMM') AS MonthName,
               CAST(QUARTER(FullDate) AS INT) AS QuarterNumber,
               CAST(YEAR(FullDate) AS INT) AS YearNumber,
               CAST(DAYOFWEEK(FullDate) AS INT) AS DayOfWeekNumber,
               DATE_FORMAT(FullDate,'EEEE') AS DayOfWeekName,
               CAST(CASE WHEN DAYOFWEEK(FullDate) IN (1,7) THEN 1 ELSE 0 END AS INT) AS IsWeekend
        FROM d
    """)

    # ---------------------------------------------------------
    # 3. Ghi kết quả ra tầng Gold (Định dạng Parquet)
    # ---------------------------------------------------------
    write_parquet(dim_patient, f"{gold_base}/Dim_Patient")
    write_parquet(dim_doctor, f"{gold_base}/Dim_Doctor")
    write_parquet(dim_department, f"{gold_base}/Dim_Department")
    write_parquet(dim_service, f"{gold_base}/Dim_Service")
    write_parquet(dim_medicine, f"{gold_base}/Dim_Medicine")
    write_parquet(dim_insurance, f"{gold_base}/Dim_Insurance")
    write_parquet(dim_date, f"{gold_base}/Dim_Date")

    print("Dimension job completed successfully.")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: hospital_dimension_job.py <YYYY-MM-DD>")
        sys.exit(1)
    main(sys.argv[1])