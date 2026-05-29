# -*- coding: utf-8 -*-
"""
Job Trích xuất Dữ liệu (Extract Job).
Đọc toàn bộ các bảng từ hệ thống nguồn (MySQL) và ghi thẳng xuống 
tầng Bronze của HDFS dưới định dạng Parquet để lưu trữ thô (Raw Data Lake).
"""
import os
import sys
from hospital_utils import create_spark, read_mysql_table, write_parquet


def main(run_date: str):
    """
    Hàm thực thi chính của Extract Job.
    
    Args:
        run_date (str): Ngày chạy ETL, dùng để phân vùng thư mục HDFS.
    """
    spark = create_spark("ExtractMySQLToHDFS")
    source_jdbc_url = os.getenv(
        "MYSQL_SOURCE_URL",
        "jdbc:mysql://host.docker.internal:3306/hospital_source?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"
    )
    source_user = os.getenv("MYSQL_SOURCE_USER", "root")
    source_password = os.getenv("MYSQL_SOURCE_PASSWORD", "198074")
    hdfs_bronze_base = f"hdfs://hdfs-namenode:8020/hospital_etl/bronze/run_date={run_date}"

    table_mapping = {
        "patients": "patients_raw",
        "doctors": "doctors_raw",
        "departments": "departments_raw",
        "services": "services_raw",
        "medicines": "medicines_raw",
        "insurance": "insurance_raw",
        "visits": "visits_raw",
        "service_transactions": "service_transactions_raw",
        "prescriptions": "prescriptions_raw",
    }

    for mysql_table, hdfs_name in table_mapping.items():
        print(f"Extracting MySQL table: {mysql_table}")
        df = read_mysql_table(spark, source_jdbc_url, mysql_table, source_user, source_password)
        write_parquet(df, f"{hdfs_bronze_base}/{hdfs_name}")
        print(f"Written to HDFS bronze: {hdfs_bronze_base}/{hdfs_name}")

    print("Extract MySQL to HDFS completed successfully.")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: extract_mysql_to_hdfs.py <YYYY-MM-DD>")
        sys.exit(1)
    main(sys.argv[1])
