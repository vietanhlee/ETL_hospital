# -*- coding: utf-8 -*-
"""
Job Làm sạch và Chuẩn hóa dữ liệu (Staging Job).
Job này đọc dữ liệu thô (Bronze), chuẩn hóa tên cột, ép kiểu dữ liệu, 
loại bỏ dữ liệu rác, và ghi dữ liệu sạch ra tầng Silver dưới dạng Parquet.
Đồng thời, đẩy các bản ghi không hợp lệ vào thư mục Quarantine.
"""

import re
import sys
from pyspark.sql import functions as F
from hospital_utils import create_spark, read_parquet, write_parquet, write_error


def normalize_name(name: str) -> str:
    """
    Chuẩn hóa chuỗi ký tự bằng cách loại bỏ các ký tự đặc biệt,
    chỉ giữ lại chữ cái (a-z) và số (0-9).
    Dùng để so khớp tên cột linh hoạt (case-insensitive & ignore symbols).
    
    Args:
        name (str): Chuỗi gốc cần chuẩn hóa.
        
    Returns:
        str: Chuỗi đã được chuẩn hóa.
    """
    return re.sub(r"[^a-z0-9]", "", name.lower())


def add_canonical_columns(df, alias_map):
    """
    Chuẩn hóa tên cột từ raw/source về một schema ổn định.
    Ví dụ: prescription_no, PrescriptionNo, PrescriptionCode -> prescription_code.
    """
    normalized_to_actual = {normalize_name(c): c for c in df.columns}

    for canonical_col, aliases in alias_map.items():
        actual_col = None
        for alias in [canonical_col] + aliases:
            key = normalize_name(alias)
            if key in normalized_to_actual:
                actual_col = normalized_to_actual[key]
                break

        if actual_col:
            df = df.withColumn(canonical_col, F.col(f"`{actual_col}`").cast("string"))
        else:
            df = df.withColumn(canonical_col, F.lit(None).cast("string"))

    return df


def main(run_date: str):
    """
    Hàm thực thi chính của Staging Job.
    
    Quy trình:
    1. Đọc dữ liệu thô từ HDFS (tầng Bronze).
    2. Ánh xạ (Map) tên cột lộn xộn về chuẩn chung (Canonical columns).
    3. Dùng SQL để làm sạch, tính toán trường phái sinh (WaitingMinutes), ép kiểu Date/Decimal.
    4. Ghi kết quả sạch ra HDFS (tầng Silver).
    5. Ghi các bản ghi lỗi/thiếu key ra thư mục Quarantine.
    
    Args:
        run_date (str): Ngày chạy ETL.
    """
    spark = create_spark("HospitalStagingJob")

    bronze_base = f"hdfs://hdfs-namenode:8020/hospital_etl/bronze/run_date={run_date}"
    silver_base = f"hdfs://hdfs-namenode:8020/hospital_etl/silver/run_date={run_date}"
    quarantine_base = f"hdfs://hdfs-namenode:8020/hospital_etl/quarantine/run_date={run_date}"

    raw_aliases = {
        "patients_raw": {
            "patient_id": ["PatientId", "PatientID"],
            "patient_code": ["PatientCode", "PatientNo", "patient_no"],
            "full_name": ["FullName", "PatientName", "patient_name"],
            "gender_raw": ["Gender", "gender"],
            "date_of_birth": ["DateOfBirth", "DOB", "dob", "birth_date"],
            "birth_year": ["BirthYear"],
            "phone": ["Phone", "PhoneNumber"],
            "address": ["Address"],
            "city_raw": ["City", "city"],
            "national_id": ["NationalId", "NationalID"],
            "source_system": ["SourceSystem"],
            "created_at": ["CreatedAt"],
            "updated_at": ["UpdatedAt"],
        },
        "doctors_raw": {
            "doctor_id": ["DoctorId", "DoctorID"],
            "doctor_code": ["DoctorCode", "DoctorNo", "doctor_no"],
            "doctor_name": ["DoctorName", "FullName"],
            "gender_raw": ["Gender", "gender"],
            "specialty_raw": ["Specialty", "specialty"],
            "academic_title_raw": ["AcademicTitle", "academic_title"],
            "department_code": ["DepartmentCode", "DepartmentNo", "department_no"],
            "status": ["Status"],
            "created_at": ["CreatedAt"],
            "updated_at": ["UpdatedAt"],
        },
        "departments_raw": {
            "department_id": ["DepartmentId", "DepartmentID"],
            "department_code": ["DepartmentCode", "DepartmentNo", "department_no"],
            "department_name_raw": ["DepartmentName", "department_name"],
            "department_group_raw": ["DepartmentGroup", "department_group"],
            "floor": ["Floor"],
            "building": ["Building"],
            "status": ["Status"],
            "created_at": ["CreatedAt"],
            "updated_at": ["UpdatedAt"],
        },
        "services_raw": {
            "service_id": ["ServiceId", "ServiceID"],
            "service_code": ["ServiceCode", "ServiceNo", "service_no"],
            "service_name_raw": ["ServiceName", "service_name"],
            "service_type_raw": ["ServiceType", "service_type"],
            "standard_price": ["StandardPrice", "standard_price_raw"],
            "status": ["Status"],
            "created_at": ["CreatedAt"],
            "updated_at": ["UpdatedAt"],
        },
        "medicines_raw": {
            "medicine_id": ["MedicineId", "MedicineID"],
            "medicine_code": ["MedicineCode", "MedicineNo", "medicine_no"],
            "medicine_name_raw": ["MedicineName", "medicine_name"],
            "medicine_group_raw": ["MedicineGroup", "medicine_group"],
            "unit_raw": ["Unit", "unit"],
            "standard_price": ["StandardPrice", "standard_price_raw"],
            "status": ["Status"],
            "created_at": ["CreatedAt"],
            "updated_at": ["UpdatedAt"],
        },
        "insurance_raw": {
            "insurance_id": ["InsuranceId", "InsuranceID"],
            "insurance_code": ["InsuranceCode", "InsuranceNo", "insurance_no"],
            "insurance_type_raw": ["InsuranceType", "insurance_type"],
            "coverage_rate_raw": ["CoverageRate", "coverage_rate"],
            "status": ["Status"],
            "created_at": ["CreatedAt"],
            "updated_at": ["UpdatedAt"],
        },
        "visits_raw": {
            "visit_id": ["VisitId", "VisitID"],
            "visit_code": ["VisitCode", "VisitNo", "visit_no"],
            "visit_date": ["VisitDate"],
            "patient_code": ["PatientCode", "PatientNo", "patient_no"],
            "doctor_code": ["DoctorCode", "DoctorNo", "doctor_no"],
            "department_code": ["DepartmentCode", "DepartmentNo", "department_no"],
            "insurance_code": ["InsuranceCode", "InsuranceNo", "insurance_no"],
            "checkin_time": ["CheckinTime", "CheckInTime", "check_in_time"],
            "consultation_start_time": ["ConsultationStartTime", "consultation_start"],
            "consultation_fee_raw": ["ConsultationFeeRaw", "ConsultationFee"],
            "visit_status": ["VisitStatus"],
            "source_system": ["SourceSystem"],
            "created_at": ["CreatedAt"],
            "updated_at": ["UpdatedAt"],
        },
        "service_transactions_raw": {
            "service_transaction_id": ["ServiceTransactionId", "ServiceTransactionID"],
            "service_transaction_code": ["ServiceTransactionCode", "ServiceTransactionNo", "service_transaction_no"],
            "visit_code": ["VisitCode", "VisitNo", "visit_no"],
            "service_date": ["ServiceDate"],
            "patient_code": ["PatientCode", "PatientNo", "patient_no"],
            "doctor_code": ["DoctorCode", "DoctorNo", "doctor_no"],
            "department_code": ["DepartmentCode", "DepartmentNo", "department_no"],
            "service_code": ["ServiceCode", "ServiceNo", "service_no"],
            "insurance_code": ["InsuranceCode", "InsuranceNo", "insurance_no"],
            "quantity_raw": ["QuantityRaw", "Quantity"],
            "unit_price_raw": ["UnitPriceRaw", "UnitPrice"],
            "total_amount_raw": ["TotalAmountRaw", "TotalAmount", "RevenueAmountRaw", "RevenueAmount"],
            "insurance_paid_raw": ["InsurancePaidRaw", "InsurancePaidAmount"],
            "patient_paid_raw": ["PatientPaidRaw", "PatientPaidAmount"],
            "payment_status": ["PaymentStatus"],
            "created_at": ["CreatedAt"],
            "updated_at": ["UpdatedAt"],
        },
        "prescriptions_raw": {
            "prescription_id": ["PrescriptionId", "PrescriptionID"],
            "prescription_code": ["PrescriptionCode", "PrescriptionNo", "prescription_no"],
            "prescription_line_no": ["PrescriptionLineNo", "prescription_line_no"],
            "visit_code": ["VisitCode", "VisitNo", "visit_no"],
            "prescription_date": ["PrescriptionDate"],
            "patient_code": ["PatientCode", "PatientNo", "patient_no"],
            "doctor_code": ["DoctorCode", "DoctorNo", "doctor_no"],
            "department_code": ["DepartmentCode", "DepartmentNo", "department_no"],
            "medicine_code": ["MedicineCode", "MedicineNo", "medicine_no"],
            "insurance_code": ["InsuranceCode", "InsuranceNo", "insurance_no"],
            "quantity_raw": ["QuantityRaw", "Quantity"],
            "unit_price_raw": ["UnitPriceRaw", "UnitPrice"],
            "total_amount_raw": ["TotalAmountRaw", "TotalAmount", "MedicineRevenueAmountRaw", "MedicineRevenueAmount"],
            "insurance_paid_raw": ["InsurancePaidRaw", "InsurancePaidAmount"],
            "patient_paid_raw": ["PatientPaidRaw", "PatientPaidAmount"],
            "dispense_status": ["DispenseStatus"],
            "created_at": ["CreatedAt"],
            "updated_at": ["UpdatedAt"],
        },
    }

    for name, alias_map in raw_aliases.items():
        df = read_parquet(spark, f"{bronze_base}/{name}")
        df = add_canonical_columns(df, alias_map)
        df.createOrReplaceTempView(name)

    money_expr = lambda col_name: f"""
        CAST(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        UPPER(TRIM(CAST({col_name} AS STRING))),
                        'VND', ''
                    ),
                    ',', ''
                ),
                ' ', ''
            )
        AS DECIMAL(18,2))
    """

    # -------------------------
    # Master data cleansing
    # -------------------------
    stg_patients = spark.sql("""
        WITH standardized AS (
            SELECT
                UPPER(TRIM(patient_code)) AS PatientCode,
                INITCAP(LOWER(TRIM(REGEXP_REPLACE(full_name, '\\\\s+', ' ')))) AS FullName,
                CASE
                    WHEN LOWER(TRIM(gender_raw)) IN ('nam','m','male','man','boy','nam giới') THEN 'Nam'
                    WHEN LOWER(TRIM(gender_raw)) IN ('nữ','nu','f','female','woman','girl','nữ giới') THEN 'Nữ'
                    WHEN LOWER(TRIM(gender_raw)) IN ('khác','khac','other') THEN 'Khác'
                    ELSE 'Không xác định'
                END AS Gender,
                CASE
                    WHEN COALESCE(
                        CAST(birth_year AS INT),
                        YEAR(COALESCE(
                            TO_DATE(date_of_birth,'yyyy-MM-dd'),
                            TO_DATE(date_of_birth,'dd/MM/yyyy'),
                            TO_DATE(date_of_birth,'yyyyMMdd'),
                            TO_DATE(date_of_birth,'yyyy/MM/dd'),
                            TO_DATE(date_of_birth,'dd-MM-yyyy'),
                            TO_DATE(date_of_birth,'yyyy.MM.dd')
                        ))
                    ) BETWEEN 1900 AND YEAR(CURRENT_DATE())
                    THEN COALESCE(
                        CAST(birth_year AS INT),
                        YEAR(COALESCE(
                            TO_DATE(date_of_birth,'yyyy-MM-dd'),
                            TO_DATE(date_of_birth,'dd/MM/yyyy'),
                            TO_DATE(date_of_birth,'yyyyMMdd'),
                            TO_DATE(date_of_birth,'yyyy/MM/dd'),
                            TO_DATE(date_of_birth,'dd-MM-yyyy'),
                            TO_DATE(date_of_birth,'yyyy.MM.dd')
                        ))
                    )
                    ELSE NULL
                END AS BirthYear,
                CASE
                    WHEN LOWER(TRIM(city_raw)) IN ('ha noi','hà nội','hn','hanoi') THEN 'Hà Nội'
                    WHEN LOWER(TRIM(city_raw)) IN ('hcm','tp.hcm','tp hcm','tphcm','tp. hcm','tp. hồ chí minh','ho chi minh','hồ chí minh','sai gon','sài gòn','sg') THEN 'TP. Hồ Chí Minh'
                    WHEN LOWER(TRIM(city_raw)) IN ('da nang','đà nẵng') THEN 'Đà Nẵng'
                    WHEN LOWER(TRIM(city_raw)) IN ('hai phong','hải phòng','hp') THEN 'Hải Phòng'
                    WHEN LOWER(TRIM(city_raw)) IN ('bac ninh','bắc ninh') THEN 'Bắc Ninh'
                    WHEN LOWER(TRIM(city_raw)) IN ('can tho','cần thơ') THEN 'Cần Thơ'
                    WHEN LOWER(TRIM(city_raw)) IN ('nghe an','nghệ an') THEN 'Nghệ An'
                    WHEN LOWER(TRIM(city_raw)) IN ('thanh hoa','thanh hóa','thanh hoá') THEN 'Thanh Hóa'
                    ELSE INITCAP(LOWER(TRIM(REGEXP_REPLACE(city_raw, '\\\\s+', ' '))))
                END AS City,
                updated_at,
                patient_id
            FROM patients_raw
            WHERE patient_code IS NOT NULL AND TRIM(patient_code) <> ''
        )
        SELECT
            PatientCode,
            FullName,
            Gender,
            BirthYear,
            CASE
                WHEN BirthYear IS NULL THEN 'Không xác định'
                WHEN YEAR(CURRENT_DATE()) - BirthYear <= 17 THEN '0-17'
                WHEN YEAR(CURRENT_DATE()) - BirthYear <= 35 THEN '18-35'
                WHEN YEAR(CURRENT_DATE()) - BirthYear <= 55 THEN '36-55'
                ELSE '56+'
            END AS AgeGroup,
            City,
            updated_at,
            patient_id
        FROM standardized
    """)

    stg_doctors = spark.sql("""
        SELECT
            UPPER(TRIM(doctor_code)) AS DoctorCode,
            INITCAP(LOWER(TRIM(REGEXP_REPLACE(doctor_name, '\\\\s+', ' ')))) AS DoctorName,
            CASE
                WHEN LOWER(TRIM(gender_raw)) IN ('nam','m','male','man','boy','nam giới') THEN 'Nam'
                WHEN LOWER(TRIM(gender_raw)) IN ('nữ','nu','f','female','woman','girl','nữ giới') THEN 'Nữ'
                ELSE 'Không xác định'
            END AS Gender,
            CASE
                WHEN LOWER(TRIM(specialty_raw)) IN ('tim mach','tim mạch','cardiology') THEN 'Tim mạch'
                WHEN LOWER(TRIM(specialty_raw)) IN ('nhi khoa') THEN 'Nhi khoa'
                WHEN LOWER(TRIM(specialty_raw)) IN ('chan doan hinh anh','chẩn đoán hình ảnh') THEN 'Chẩn đoán hình ảnh'
                WHEN LOWER(TRIM(specialty_raw)) IN ('xet nghiem','xét nghiệm','lab') THEN 'Xét nghiệm'
                WHEN LOWER(TRIM(specialty_raw)) IN ('tai mui hong','tai mũi họng') THEN 'Tai mũi họng'
                WHEN LOWER(TRIM(specialty_raw)) IN ('cap cuu','cấp cứu') THEN 'Cấp cứu'
                WHEN LOWER(TRIM(specialty_raw)) IN ('ngoai khoa','ngoại khoa') THEN 'Ngoại khoa'
                WHEN LOWER(TRIM(specialty_raw)) IN ('san khoa','sản khoa') THEN 'Sản khoa'
                WHEN LOWER(TRIM(specialty_raw)) IN ('noi tong quat','nội tổng quát','nội tổng hợp','noi tong hop') THEN 'Nội tổng quát'
                WHEN LOWER(TRIM(specialty_raw)) IN ('da lieu','da liễu') THEN 'Da liễu'
                ELSE INITCAP(LOWER(TRIM(REGEXP_REPLACE(specialty_raw, '\\\\s+', ' '))))
            END AS Specialty,
            CASE
                WHEN LOWER(TRIM(academic_title_raw)) IN ('bs','bác sĩ','bac si') THEN 'BS'
                WHEN LOWER(TRIM(academic_title_raw)) IN ('ths','thạc sĩ','thac si') THEN 'ThS'
                WHEN LOWER(TRIM(academic_title_raw)) IN ('ts','tiến sĩ','tien si') THEN 'TS'
                ELSE TRIM(academic_title_raw)
            END AS AcademicTitle,
            updated_at,
            doctor_id
        FROM doctors_raw
        WHERE doctor_code IS NOT NULL AND TRIM(doctor_code) <> ''
    """)

    stg_departments = spark.sql("""
        SELECT
            UPPER(TRIM(department_code)) AS DepartmentCode,
            INITCAP(LOWER(TRIM(REGEXP_REPLACE(department_name_raw, '\\\\s+', ' ')))) AS DepartmentName,
            CASE
                WHEN LOWER(TRIM(department_group_raw)) IN ('kham benh','khám bệnh') THEN 'Khám bệnh'
                WHEN LOWER(TRIM(department_group_raw)) IN ('cls','can lam sang','cận lâm sàng') THEN 'Cận lâm sàng'
                ELSE INITCAP(LOWER(TRIM(REGEXP_REPLACE(department_group_raw, '\\\\s+', ' '))))
            END AS DepartmentGroup,
            updated_at,
            department_id
        FROM departments_raw
        WHERE department_code IS NOT NULL AND TRIM(department_code) <> ''
    """)

    stg_services = spark.sql(f"""
        SELECT
            UPPER(TRIM(service_code)) AS ServiceCode,
            INITCAP(LOWER(TRIM(REGEXP_REPLACE(service_name_raw, '\\\\s+', ' ')))) AS ServiceName,
            CASE
                WHEN LOWER(TRIM(service_type_raw)) IN ('kham','khám') THEN 'Khám'
                WHEN LOWER(TRIM(service_type_raw)) IN ('xet nghiem','xét nghiệm','lab') THEN 'Xét nghiệm'
                WHEN LOWER(TRIM(service_type_raw)) IN ('chan doan hinh anh','chẩn đoán hình ảnh') THEN 'Chẩn đoán hình ảnh'
                WHEN LOWER(TRIM(service_type_raw)) IN ('thu thuat','thủ thuật') THEN 'Thủ thuật'
                ELSE INITCAP(LOWER(TRIM(REGEXP_REPLACE(service_type_raw, '\\\\s+', ' '))))
            END AS ServiceType,
            {money_expr('standard_price')} AS StandardPrice,
            updated_at,
            service_id
        FROM services_raw
        WHERE service_code IS NOT NULL AND TRIM(service_code) <> ''
    """)

    stg_medicines = spark.sql(f"""
        SELECT
            UPPER(TRIM(medicine_code)) AS MedicineCode,
            INITCAP(LOWER(TRIM(REGEXP_REPLACE(medicine_name_raw, '\\\\s+', ' ')))) AS MedicineName,
            CASE
                WHEN LOWER(TRIM(medicine_group_raw)) IN ('khang sinh','kháng sinh','antibiotic') THEN 'Kháng sinh'
                WHEN LOWER(TRIM(medicine_group_raw)) IN ('giam dau','giảm đau') THEN 'Giảm đau'
                WHEN LOWER(TRIM(medicine_group_raw)) IN ('tieu hoa','tiêu hóa','dạ dày','da day') THEN 'Tiêu hóa'
                WHEN LOWER(TRIM(medicine_group_raw)) IN ('ho hap','hô hấp') THEN 'Hô hấp'
                WHEN LOWER(TRIM(medicine_group_raw)) IN ('tim mach','tim mạch') THEN 'Tim mạch'
                WHEN LOWER(TRIM(medicine_group_raw)) IN ('noi tiet','nội tiết') THEN 'Nội tiết'
                WHEN LOWER(TRIM(medicine_group_raw)) IN ('khang histamin','kháng histamin') THEN 'Kháng histamin'
                WHEN LOWER(TRIM(medicine_group_raw)) IN ('khang viem','kháng viêm') THEN 'Kháng viêm'
                WHEN LOWER(TRIM(medicine_group_raw)) IN ('bu dien giai','bù điện giải') THEN 'Bù điện giải'
                ELSE INITCAP(LOWER(TRIM(REGEXP_REPLACE(medicine_group_raw, '\\\\s+', ' '))))
            END AS MedicineGroup,
            CASE
                WHEN LOWER(TRIM(unit_raw)) IN ('vien','viên','tablet') THEN 'Viên'
                WHEN LOWER(TRIM(unit_raw)) IN ('hop','hộp','box') THEN 'Hộp'
                WHEN LOWER(TRIM(unit_raw)) IN ('chai','bottle') THEN 'Chai'
                WHEN LOWER(TRIM(unit_raw)) IN ('ong','ống','ampoule') THEN 'Ống'
                WHEN LOWER(TRIM(unit_raw)) IN ('goi','gói') THEN 'Gói'
                ELSE INITCAP(LOWER(TRIM(REGEXP_REPLACE(unit_raw, '\\\\s+', ' '))))
            END AS Unit,
            {money_expr('standard_price')} AS StandardPrice,
            updated_at,
            medicine_id
        FROM medicines_raw
        WHERE medicine_code IS NOT NULL AND TRIM(medicine_code) <> ''
    """)

    stg_insurance = spark.sql("""
        WITH rate AS (
            SELECT
                UPPER(TRIM(insurance_code)) AS InsuranceCode,
                CASE
                    WHEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST(coverage_rate_raw AS STRING)), '%', ''), ' ', '') AS DOUBLE) <= 1
                    THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST(coverage_rate_raw AS STRING)), '%', ''), ' ', '') AS DOUBLE) * 100
                    ELSE CAST(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST(coverage_rate_raw AS STRING)), '%', ''), ' ', '') AS DOUBLE)
                END AS CoverageRate,
                updated_at,
                insurance_id
            FROM insurance_raw
            WHERE insurance_code IS NOT NULL AND TRIM(insurance_code) <> ''
        )
        SELECT
            InsuranceCode,
            CASE
                WHEN CoverageRate = 0 THEN 'Tự chi trả'
                ELSE CONCAT('BHYT ', CAST(CAST(CoverageRate AS INT) AS STRING), '%')
            END AS InsuranceType,
            CAST(CoverageRate AS DECIMAL(5,2)) AS CoverageRate,
            updated_at,
            insurance_id
        FROM rate
        WHERE CoverageRate BETWEEN 0 AND 100
    """)

    stg_patients.createOrReplaceTempView("stg_patients_tmp")
    stg_doctors.createOrReplaceTempView("stg_doctors_tmp")
    stg_departments.createOrReplaceTempView("stg_departments_tmp")
    stg_services.createOrReplaceTempView("stg_services_tmp")
    stg_medicines.createOrReplaceTempView("stg_medicines_tmp")
    stg_insurance.createOrReplaceTempView("stg_insurance_tmp")

    # -------------------------
    # Transaction cleansing
    # -------------------------
    visits_parsed = spark.sql(f"""
        SELECT
            visit_id,
            UPPER(TRIM(visit_code)) AS VisitCode,
            COALESCE(
                TO_DATE(visit_date,'yyyy-MM-dd'),
                TO_DATE(visit_date,'dd/MM/yyyy'),
                TO_DATE(visit_date,'yyyyMMdd'),
                TO_DATE(visit_date,'yyyy/MM/dd'),
                TO_DATE(visit_date,'dd-MM-yyyy'),
                TO_DATE(visit_date,'yyyy.MM.dd'),
                TO_DATE(CAST(checkin_time AS TIMESTAMP)),
                TO_DATE(CAST(created_at AS TIMESTAMP))
            ) AS VisitDate,
            UPPER(TRIM(patient_code)) AS PatientCode,
            UPPER(TRIM(doctor_code)) AS DoctorCode,
            UPPER(TRIM(department_code)) AS DepartmentCode,
            UPPER(TRIM(insurance_code)) AS InsuranceCode,
            CASE
                WHEN checkin_time IS NOT NULL
                     AND consultation_start_time IS NOT NULL
                     AND UNIX_TIMESTAMP(CAST(consultation_start_time AS STRING)) IS NOT NULL
                     AND UNIX_TIMESTAMP(CAST(checkin_time AS STRING)) IS NOT NULL
                THEN GREATEST(
                    CAST((UNIX_TIMESTAMP(CAST(consultation_start_time AS STRING)) - UNIX_TIMESTAMP(CAST(checkin_time AS STRING))) / 60 AS INT),
                    0
                )
                ELSE 0
            END AS WaitingMinutes,
            {money_expr('consultation_fee_raw')} AS ConsultationFee,
            UPPER(TRIM(COALESCE(visit_status, 'UNKNOWN'))) AS VisitStatus,
            created_at,
            updated_at
        FROM visits_raw
    """)
    visits_parsed.createOrReplaceTempView("visits_parsed")

    stg_visits = spark.sql("""
        SELECT *
        FROM visits_parsed
        WHERE VisitCode IS NOT NULL AND TRIM(VisitCode) <> ''
          AND VisitDate IS NOT NULL
          AND ConsultationFee IS NOT NULL AND ConsultationFee >= 0
          AND WaitingMinutes >= 0
          AND VisitStatus = 'COMPLETED'
    """)
    stg_visits.createOrReplaceTempView("stg_visits_tmp")

    service_transactions_parsed = spark.sql(f"""
        WITH parsed AS (
            SELECT
                service_transaction_id,
                UPPER(TRIM(service_transaction_code)) AS ServiceTransactionCode,
                UPPER(TRIM(visit_code)) AS VisitCode,
                COALESCE(
                    TO_DATE(service_date,'yyyy-MM-dd'),
                    TO_DATE(service_date,'dd/MM/yyyy'),
                    TO_DATE(service_date,'yyyyMMdd'),
                    TO_DATE(service_date,'yyyy/MM/dd'),
                    TO_DATE(service_date,'dd-MM-yyyy'),
                    TO_DATE(service_date,'yyyy.MM.dd'),
                    TO_DATE(CAST(created_at AS TIMESTAMP))
                ) AS ServiceDate,
                UPPER(TRIM(patient_code)) AS PatientCode,
                UPPER(TRIM(doctor_code)) AS DoctorCode,
                UPPER(TRIM(department_code)) AS DepartmentCode,
                UPPER(TRIM(service_code)) AS ServiceCode,
                UPPER(TRIM(insurance_code)) AS InsuranceCode,
                CAST(REGEXP_REPLACE(TRIM(CAST(quantity_raw AS STRING)), ',', '') AS INT) AS QuantityRawParsed,
                {money_expr('unit_price_raw')} AS UnitPriceRawParsed,
                {money_expr('total_amount_raw')} AS TotalAmountRawParsed,
                UPPER(TRIM(COALESCE(payment_status, 'UNKNOWN'))) AS PaymentStatus,
                created_at,
                updated_at
            FROM service_transactions_raw
        ),
        recovered AS (
            SELECT
                p.*,
                CASE
                    WHEN QuantityRawParsed IS NOT NULL AND QuantityRawParsed > 0 THEN QuantityRawParsed
                    WHEN TotalAmountRawParsed IS NOT NULL AND TotalAmountRawParsed > 0
                         AND UnitPriceRawParsed IS NOT NULL AND UnitPriceRawParsed > 0
                    THEN GREATEST(CAST(ROUND(TotalAmountRawParsed / UnitPriceRawParsed, 0) AS INT), 1)
                    ELSE 1
                END AS QuantityClean
            FROM parsed p
        )
        SELECT
            r.*,
            CASE
                WHEN UnitPriceRawParsed IS NOT NULL AND UnitPriceRawParsed > 0 THEN UnitPriceRawParsed
                WHEN TotalAmountRawParsed IS NOT NULL AND TotalAmountRawParsed > 0 AND QuantityClean > 0
                THEN CAST(TotalAmountRawParsed / QuantityClean AS DECIMAL(18,2))
                ELSE CAST(0 AS DECIMAL(18,2))
            END AS UnitPriceClean
        FROM recovered r
    """)
    service_transactions_parsed.createOrReplaceTempView("service_transactions_parsed")

    stg_service_transactions = spark.sql("""
        SELECT
            ServiceTransactionCode,
            VisitCode,
            ServiceDate,
            PatientCode,
            DoctorCode,
            DepartmentCode,
            ServiceCode,
            InsuranceCode,
            QuantityClean AS Quantity,
            UnitPriceClean AS UnitPrice,
            CAST(QuantityClean * UnitPriceClean AS DECIMAL(18,2)) AS RevenueAmount,
            PaymentStatus,
            created_at,
            updated_at,
            service_transaction_id
        FROM service_transactions_parsed
        WHERE ServiceTransactionCode IS NOT NULL AND TRIM(ServiceTransactionCode) <> ''
          AND VisitCode IS NOT NULL AND TRIM(VisitCode) <> ''
          AND ServiceDate IS NOT NULL
          AND ServiceCode IS NOT NULL AND TRIM(ServiceCode) <> ''
          AND QuantityClean > 0
          AND UnitPriceClean > 0
          AND PaymentStatus = 'PAID'
    """)

    prescriptions_parsed = spark.sql(f"""
        WITH parsed AS (
            SELECT
                prescription_id,
                UPPER(TRIM(prescription_code)) AS PrescriptionCode,
                UPPER(TRIM(visit_code)) AS VisitCode,
                COALESCE(
                    TO_DATE(prescription_date,'yyyy-MM-dd'),
                    TO_DATE(prescription_date,'dd/MM/yyyy'),
                    TO_DATE(prescription_date,'yyyyMMdd'),
                    TO_DATE(prescription_date,'yyyy/MM/dd'),
                    TO_DATE(prescription_date,'dd-MM-yyyy'),
                    TO_DATE(prescription_date,'yyyy.MM.dd'),
                    TO_DATE(CAST(created_at AS TIMESTAMP))
                ) AS PrescriptionDate,
                UPPER(TRIM(patient_code)) AS PatientCode,
                UPPER(TRIM(doctor_code)) AS DoctorCode,
                UPPER(TRIM(department_code)) AS DepartmentCode,
                UPPER(TRIM(medicine_code)) AS MedicineCode,
                UPPER(TRIM(insurance_code)) AS InsuranceCode,
                CAST(REGEXP_REPLACE(TRIM(CAST(quantity_raw AS STRING)), ',', '') AS INT) AS QuantityRawParsed,
                {money_expr('unit_price_raw')} AS UnitPriceRawParsed,
                {money_expr('total_amount_raw')} AS TotalAmountRawParsed,
                UPPER(TRIM(COALESCE(dispense_status, 'UNKNOWN'))) AS DispenseStatus,
                created_at,
                updated_at
            FROM prescriptions_raw
        ),
        recovered AS (
            SELECT
                p.*,
                CASE
                    WHEN QuantityRawParsed IS NOT NULL AND QuantityRawParsed > 0 THEN QuantityRawParsed
                    WHEN TotalAmountRawParsed IS NOT NULL AND TotalAmountRawParsed > 0
                         AND UnitPriceRawParsed IS NOT NULL AND UnitPriceRawParsed > 0
                    THEN GREATEST(CAST(ROUND(TotalAmountRawParsed / UnitPriceRawParsed, 0) AS INT), 1)
                    ELSE 1
                END AS QuantityClean
            FROM parsed p
        )
        SELECT
            r.*,
            CASE
                WHEN UnitPriceRawParsed IS NOT NULL AND UnitPriceRawParsed > 0 THEN UnitPriceRawParsed
                WHEN TotalAmountRawParsed IS NOT NULL AND TotalAmountRawParsed > 0 AND QuantityClean > 0
                THEN CAST(TotalAmountRawParsed / QuantityClean AS DECIMAL(18,2))
                ELSE CAST(0 AS DECIMAL(18,2))
            END AS UnitPriceClean
        FROM recovered r
    """)
    prescriptions_parsed.createOrReplaceTempView("prescriptions_parsed")

    stg_prescriptions = spark.sql("""
        SELECT
            pr.PrescriptionCode,
            pr.VisitCode,
            COALESCE(pr.PrescriptionDate, v.VisitDate) AS PrescriptionDate,
            COALESCE(CASE WHEN TRIM(pr.PatientCode) <> '' THEN pr.PatientCode END, v.PatientCode) AS PatientCode,
            COALESCE(CASE WHEN TRIM(pr.DoctorCode) <> '' THEN pr.DoctorCode END, v.DoctorCode) AS DoctorCode,
            COALESCE(CASE WHEN TRIM(pr.DepartmentCode) <> '' THEN pr.DepartmentCode END, v.DepartmentCode) AS DepartmentCode,
            pr.MedicineCode,
            COALESCE(CASE WHEN TRIM(pr.InsuranceCode) <> '' THEN pr.InsuranceCode END, v.InsuranceCode) AS InsuranceCode,
            pr.QuantityClean AS Quantity,
            pr.UnitPriceClean AS UnitPrice,
            CAST(pr.QuantityClean * pr.UnitPriceClean AS DECIMAL(18,2)) AS MedicineRevenueAmount,
            pr.DispenseStatus,
            pr.created_at,
            pr.updated_at,
            pr.prescription_id
        FROM prescriptions_parsed pr
        LEFT JOIN stg_visits_tmp v ON pr.VisitCode = v.VisitCode
        WHERE pr.PrescriptionCode IS NOT NULL AND TRIM(pr.PrescriptionCode) <> ''
          AND pr.VisitCode IS NOT NULL AND TRIM(pr.VisitCode) <> ''
          AND COALESCE(pr.PrescriptionDate, v.VisitDate) IS NOT NULL
          AND pr.MedicineCode IS NOT NULL AND TRIM(pr.MedicineCode) <> ''
          AND pr.QuantityClean > 0
          AND pr.UnitPriceClean > 0
          AND pr.DispenseStatus = 'DISPENSED'
    """)

    # -------------------------
    # Error layer
    # -------------------------
    error_queries = {
        "patients_invalid": """
            SELECT *, 'missing_patient_code' AS error_reason
            FROM patients_raw
            WHERE patient_code IS NULL OR TRIM(patient_code) = ''
        """,
        "doctors_invalid": """
            SELECT *, 'missing_doctor_code' AS error_reason
            FROM doctors_raw
            WHERE doctor_code IS NULL OR TRIM(doctor_code) = ''
        """,
        "departments_invalid": """
            SELECT *, 'missing_department_code' AS error_reason
            FROM departments_raw
            WHERE department_code IS NULL OR TRIM(department_code) = ''
        """,
        "services_invalid": """
            SELECT *, 'missing_service_code' AS error_reason
            FROM services_raw
            WHERE service_code IS NULL OR TRIM(service_code) = ''
        """,
        "medicines_invalid": """
            SELECT *, 'missing_medicine_code' AS error_reason
            FROM medicines_raw
            WHERE medicine_code IS NULL OR TRIM(medicine_code) = ''
        """,
        "insurance_invalid": """
            SELECT *, 'invalid_insurance_code_or_coverage_rate' AS error_reason
            FROM insurance_raw
            WHERE insurance_code IS NULL OR TRIM(insurance_code) = ''
        """,
        "visits_invalid": """
            SELECT *,
                CASE
                    WHEN VisitCode IS NULL OR TRIM(VisitCode) = '' THEN 'missing_visit_code'
                    WHEN VisitDate IS NULL THEN 'invalid_visit_date'
                    WHEN ConsultationFee IS NULL OR ConsultationFee < 0 THEN 'invalid_consultation_fee'
                    WHEN WaitingMinutes < 0 THEN 'negative_waiting_minutes'
                    WHEN VisitStatus <> 'COMPLETED' THEN 'not_completed_visit'
                    ELSE 'unknown_visit_error'
                END AS error_reason
            FROM visits_parsed
            WHERE VisitCode IS NULL OR TRIM(VisitCode) = ''
               OR VisitDate IS NULL
               OR ConsultationFee IS NULL OR ConsultationFee < 0
               OR WaitingMinutes < 0
               OR VisitStatus <> 'COMPLETED'
        """,
        "service_transactions_invalid": """
            SELECT *,
                CASE
                    WHEN ServiceTransactionCode IS NULL OR TRIM(ServiceTransactionCode) = '' THEN 'missing_service_transaction_code'
                    WHEN VisitCode IS NULL OR TRIM(VisitCode) = '' THEN 'missing_visit_code'
                    WHEN ServiceDate IS NULL THEN 'invalid_service_date'
                    WHEN ServiceCode IS NULL OR TRIM(ServiceCode) = '' THEN 'missing_service_code'
                    WHEN QuantityClean <= 0 THEN 'invalid_quantity'
                    WHEN UnitPriceClean <= 0 THEN 'invalid_unit_price'
                    WHEN PaymentStatus <> 'PAID' THEN 'not_paid_service_transaction'
                    ELSE 'unknown_service_transaction_error'
                END AS error_reason
            FROM service_transactions_parsed
            WHERE ServiceTransactionCode IS NULL OR TRIM(ServiceTransactionCode) = ''
               OR VisitCode IS NULL OR TRIM(VisitCode) = ''
               OR ServiceDate IS NULL
               OR ServiceCode IS NULL OR TRIM(ServiceCode) = ''
               OR QuantityClean <= 0
               OR UnitPriceClean <= 0
               OR PaymentStatus <> 'PAID'
        """,
        "prescriptions_invalid": """
            SELECT *,
                CASE
                    WHEN PrescriptionCode IS NULL OR TRIM(PrescriptionCode) = '' THEN 'missing_prescription_code'
                    WHEN VisitCode IS NULL OR TRIM(VisitCode) = '' THEN 'missing_visit_code'
                    WHEN PrescriptionDate IS NULL THEN 'invalid_prescription_date'
                    WHEN MedicineCode IS NULL OR TRIM(MedicineCode) = '' THEN 'missing_medicine_code'
                    WHEN QuantityClean <= 0 THEN 'invalid_quantity'
                    WHEN UnitPriceClean <= 0 THEN 'invalid_unit_price'
                    WHEN DispenseStatus <> 'DISPENSED' THEN 'not_dispensed_prescription'
                    ELSE 'unknown_prescription_error'
                END AS error_reason
            FROM prescriptions_parsed
            WHERE PrescriptionCode IS NULL OR TRIM(PrescriptionCode) = ''
               OR VisitCode IS NULL OR TRIM(VisitCode) = ''
               OR PrescriptionDate IS NULL
               OR MedicineCode IS NULL OR TRIM(MedicineCode) = ''
               OR QuantityClean <= 0
               OR UnitPriceClean <= 0
               OR DispenseStatus <> 'DISPENSED'
        """
    }

    for folder, query in error_queries.items():
        write_error(spark.sql(query), f"{quarantine_base}/{folder}")

    print("Staging counts:")
    print("stg_patients =", stg_patients.count())
    print("stg_doctors =", stg_doctors.count())
    print("stg_departments =", stg_departments.count())
    print("stg_services =", stg_services.count())
    print("stg_medicines =", stg_medicines.count())
    print("stg_insurance =", stg_insurance.count())
    print("stg_visits =", stg_visits.count())
    print("stg_service_transactions =", stg_service_transactions.count())
    print("stg_prescriptions =", stg_prescriptions.count())

    write_parquet(stg_patients, f"{silver_base}/stg_patients")
    write_parquet(stg_doctors, f"{silver_base}/stg_doctors")
    write_parquet(stg_departments, f"{silver_base}/stg_departments")
    write_parquet(stg_services, f"{silver_base}/stg_services")
    write_parquet(stg_medicines, f"{silver_base}/stg_medicines")
    write_parquet(stg_insurance, f"{silver_base}/stg_insurance")
    write_parquet(stg_visits, f"{silver_base}/stg_visits")
    write_parquet(stg_service_transactions, f"{silver_base}/stg_service_transactions")
    write_parquet(stg_prescriptions, f"{silver_base}/stg_prescriptions")

    print("Staging job completed successfully.")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: hospital_staging_job.py <YYYY-MM-DD>")
        sys.exit(1)

    main(sys.argv[1])
