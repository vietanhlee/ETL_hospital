# -*- coding: utf-8 -*-
"""
Job Nạp Dữ liệu (Load Job).
Job này khởi tạo các bảng ClickHouse sử dụng ReplacingMergeTree (hỗ trợ Upsert),
đọc dữ liệu tầng Gold từ HDFS (định dạng Parquet) và đẩy thẳng vào
ClickHouse Cloud Data Warehouse thông qua JDBC.
"""

import os
import sys
import urllib.request
from hospital_utils import create_spark, read_parquet, execute_ch_sql_http, load_to_clickhouse



def main(run_date):
    """
    Hàm thực thi chính của Load Job.
    
    Quy trình:
    1. Kết nối ClickHouse Cloud qua HTTP.
    2. Tạo Database và các bảng (ReplacingMergeTree) nếu chưa tồn tại.
    3. Đọc dữ liệu từ HDFS tầng Gold (đã là định dạng Parquet với schema chuẩn).
    4. Nạp song song dữ liệu Dimension và Fact lên ClickHouse.
    
    Args:
        run_date (str): Ngày chạy ETL.
    """
    spark = create_spark("LoadHDFSToClickHouseDW")

    gold_base = f"hdfs://hdfs-namenode:8020/hospital_etl/gold/run_date={run_date}"

    print("Connecting to ClickHouse via HTTP API...")
    # Ưu tiên đọc từ biến môi trường (cloud). Nếu không có, fallback về ClickHouse local trong Docker
    ch_host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')       # 'clickhouse' = tên container trong etl_network
    ch_port = os.getenv('CLICKHOUSE_PORT', '8123')             # 8123 = HTTP port của local ClickHouse (không dùng SSL)
    ch_secure = os.getenv('CLICKHOUSE_SECURE', 'False').lower() in ('true', '1', 't')  # Local không cần SSL
    ch_user = os.getenv('CLICKHOUSE_USER', 'default')
    ch_password = os.getenv('CLICKHOUSE_PASSWORD', '')         # Local mặc định không có password
    db_name = os.getenv('CLICKHOUSE_DB', 'hospital_dw')
    
    # Tạo DB nếu chưa có
    execute_ch_sql_http(ch_host, ch_port, ch_user, ch_password, "", f"CREATE DATABASE IF NOT EXISTS {db_name}", ch_secure)

    # 1. Khởi tạo Bảng với ReplacingMergeTree (Upsert logic)
    # ReplacingMergeTree tự động ghi đè bản ghi cũ nếu cùng sorting key (ORDER BY)
    print("Creating ClickHouse tables (ReplacingMergeTree) if not exist...")
    
    tables_ddl = {
        "dim_date": "CREATE TABLE IF NOT EXISTS dim_date (DateKey Int32, FullDate Date, DayNumber Int32, MonthNumber Int32, MonthName String, QuarterNumber Int32, YearNumber Int32, DayOfWeekNumber Int32, DayOfWeekName String, IsWeekend Int32) ENGINE = ReplacingMergeTree() ORDER BY (DateKey)",
        "dim_patient": "CREATE TABLE IF NOT EXISTS dim_patient (PatientKey Int64, PatientCode String, FullName String, Gender String, BirthYear Int32, AgeGroup String, City String) ENGINE = ReplacingMergeTree() ORDER BY (PatientCode)",
        "dim_doctor": "CREATE TABLE IF NOT EXISTS dim_doctor (DoctorKey Int64, DoctorCode String, DoctorName String, Gender String, Specialty String, AcademicTitle String) ENGINE = ReplacingMergeTree() ORDER BY (DoctorCode)",
        "dim_department": "CREATE TABLE IF NOT EXISTS dim_department (DepartmentKey Int64, DepartmentCode String, DepartmentName String, DepartmentGroup String) ENGINE = ReplacingMergeTree() ORDER BY (DepartmentCode)",
        "dim_service": "CREATE TABLE IF NOT EXISTS dim_service (ServiceKey Int64, ServiceCode String, ServiceName String, ServiceType String) ENGINE = ReplacingMergeTree() ORDER BY (ServiceCode)",
        "dim_medicine": "CREATE TABLE IF NOT EXISTS dim_medicine (MedicineKey Int64, MedicineCode String, MedicineName String, MedicineGroup String, Unit String) ENGINE = ReplacingMergeTree() ORDER BY (MedicineCode)",
        "dim_insurance": "CREATE TABLE IF NOT EXISTS dim_insurance (InsuranceKey Int64, InsuranceCode String, InsuranceType String, CoverageRate Float64) ENGINE = ReplacingMergeTree() ORDER BY (InsuranceCode)",
        "fact_visit": "CREATE TABLE IF NOT EXISTS fact_visit (VisitFactKey Int64, DateKey Int32, PatientKey Int64, DoctorKey Int64, DepartmentKey Int64, InsuranceKey Int64, VisitCode String, ConsultationFee Decimal(18,2), WaitingMinutes Int32, VisitCount Int32) ENGINE = ReplacingMergeTree() ORDER BY (VisitCode)",
        "fact_service_revenue": "CREATE TABLE IF NOT EXISTS fact_service_revenue (ServiceRevenueFactKey Int64, DateKey Int32, PatientKey Int64, DoctorKey Int64, DepartmentKey Int64, ServiceKey Int64, InsuranceKey Int64, VisitCode String, ServiceTransactionCode String, Quantity Int32, UnitPrice Decimal(18,2), RevenueAmount Decimal(18,2), InsurancePaidAmount Decimal(18,2), PatientPaidAmount Decimal(18,2)) ENGINE = ReplacingMergeTree() ORDER BY (ServiceTransactionCode)",
        "fact_prescription": "CREATE TABLE IF NOT EXISTS fact_prescription (PrescriptionFactKey Int64, DateKey Int32, PatientKey Int64, DoctorKey Int64, DepartmentKey Int64, MedicineKey Int64, InsuranceKey Int64, VisitCode String, PrescriptionCode String, Quantity Int32, UnitPrice Decimal(18,2), MedicineRevenueAmount Decimal(18,2), InsurancePaidAmount Decimal(18,2), PatientPaidAmount Decimal(18,2)) ENGINE = ReplacingMergeTree() ORDER BY (PrescriptionCode)"
    }
    
    for tbl, ddl in tables_ddl.items():
        execute_ch_sql_http(ch_host, ch_port, ch_user, ch_password, db_name, ddl, ch_secure)

    print("Reading and transforming HDFS data (Parquet formats do not need cast)...")
    
    # 2. Đọc Parquet (đã lưu sẵn kiểu dữ liệu nên không cần .cast() như CSV)
    dim_date = read_parquet(spark, f"{gold_base}/Dim_Date")
    dim_patient = read_parquet(spark, f"{gold_base}/Dim_Patient")
    dim_doctor = read_parquet(spark, f"{gold_base}/Dim_Doctor")
    dim_department = read_parquet(spark, f"{gold_base}/Dim_Department")
    dim_service = read_parquet(spark, f"{gold_base}/Dim_Service")
    dim_medicine = read_parquet(spark, f"{gold_base}/Dim_Medicine")
    dim_insurance = read_parquet(spark, f"{gold_base}/Dim_Insurance")
    fact_visit = read_parquet(spark, f"{gold_base}/Fact_Visit")
    fact_service = read_parquet(spark, f"{gold_base}/Fact_ServiceRevenue")
    fact_prescription = read_parquet(spark, f"{gold_base}/Fact_Prescription")

    print("Loading dimensions into ClickHouse DW (Upsert)...")
    load_to_clickhouse(dim_date, "dim_date", db_name)
    load_to_clickhouse(dim_patient, "dim_patient", db_name)
    load_to_clickhouse(dim_doctor, "dim_doctor", db_name)
    load_to_clickhouse(dim_department, "dim_department", db_name)
    load_to_clickhouse(dim_service, "dim_service", db_name)
    load_to_clickhouse(dim_medicine, "dim_medicine", db_name)
    load_to_clickhouse(dim_insurance, "dim_insurance", db_name)

    print("Loading facts into ClickHouse DW (Upsert)...")
    load_to_clickhouse(fact_visit, "fact_visit", db_name)
    load_to_clickhouse(fact_service, "fact_service_revenue", db_name)
    load_to_clickhouse(fact_prescription, "fact_prescription", db_name)

    print("Load HDFS gold to ClickHouse DW completed successfully.")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: load_hdfs_to_clickhouse_dw.py <YYYY-MM-DD>")
        sys.exit(1)

    main(sys.argv[1])
