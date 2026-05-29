# -*- coding: utf-8 -*-
import pymysql
import os
from pymysql.constants import CLIENT

def main():
    print("Connecting to MySQL...")
    connection = pymysql.connect(
        host='127.0.0.1',
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', 'rootpassword'),
        database=os.getenv('MYSQL_DB', 'hospital_source'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        client_flag=CLIENT.MULTI_STATEMENTS
    )

    try:
        with connection.cursor() as cursor:
            # 1. Drop Tables
            print("Dropping existing tables...")
            tables_to_drop = [
                "service_transactions",
                "prescriptions",
                "visits",
                "medicines",
                "services",
                "insurance",
                "doctors",
                "departments",
                "patients"
            ]
            
            # Tạm tắt kiểm tra khóa ngoại (nếu có) để tránh lỗi drop
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            for table in tables_to_drop:
                cursor.execute("DROP TABLE IF EXISTS " + table + ";")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            
            # 2. Lấy đường dẫn thư mục datasource
            project_dir = os.path.dirname(os.path.abspath(__file__))
            datasource_dir = os.path.join(project_dir, 'datasource')
            
            # Danh sách các file SQL cần chạy (có thể chạy theo thứ tự tạo bảng -> insert)
            sql_files = [
                "hospital_source_patients.sql",
                "hospital_source_departments.sql",
                "hospital_source_doctors.sql",
                "hospital_source_insurance.sql",
                "hospital_source_medicines.sql",
                "hospital_source_services.sql",
                "hospital_source_visits.sql",
                "hospital_source_prescriptions.sql",
                "hospital_source_service_transactions.sql"
            ]

            print("Executing SQL files from " + datasource_dir + "...")
            
            for sql_file in sql_files:
                file_path = os.path.join(datasource_dir, sql_file)
                if os.path.exists(file_path):
                    print("  - Running " + sql_file + "...")
                    with open(file_path, 'r', encoding='utf-8') as f:
                        sql_script = f.read()
                    
                    if sql_script.strip():
                        cursor.execute(sql_script)
                else:
                    print("  - Warning: File not found -> " + file_path)

            connection.commit()
            print("Source data loaded successfully from SQL files!")

    except Exception as e:
        print("Error occurred: " + str(e))
        connection.rollback()
    finally:
        connection.close()

if __name__ == '__main__':
    main()
