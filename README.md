# Hospital ETL Pipeline

Hệ thống ETL (Extract, Transform, Load) dành cho dữ liệu bệnh viện. Dự án này lấy dữ liệu từ cơ sở dữ liệu hệ thống nguồn (MySQL), xử lý qua các luồng Data Lake (HDFS) sử dụng Apache Spark (PySpark), và cuối cùng đẩy dữ liệu sạch (Data Warehouse) lên ClickHouse Cloud phục vụ cho báo cáo BI.

## 🏗 Cấu trúc thư mục dự án

```text
Hospital_ETL/
├── dags/                           # Chứa các DAG của Airflow để tự động hóa luồng chạy ETL.
│   └── hospital_etl_dag.py         # File định nghĩa luồng DAG chính
├── datasource/                     # Chứa các file dữ liệu gốc (.sql) dùng để khởi tạo Database Source.
├── docs/                           # Thư mục chứa tài liệu nghiệp vụ chi tiết của dự án.
├── scripts_final/                  # Thư mục mã nguồn PySpark lõi của dự án (Mới nhất).
│   ├── extract_mysql_to_hdfs.py    # Job 1: Extract từ MySQL đẩy thẳng vào HDFS (Bronze).
│   ├── hospital_staging_job.py     # Job 2: Làm sạch, map alias và xuất ra Silver layer.
│   ├── hospital_dimension_job.py   # Job 3: Tạo Dimension Tables (Gold layer).
│   ├── hospital_fact_job.py        # Job 4: Tạo Fact Tables (Gold layer) và xử lý Data Quality.
│   ├── load_hdfs_to_clickhouse_dw.py # Job 5: Đẩy dữ liệu từ Gold Layer lên ClickHouse DW.
│   └── hospital_utils.py           # Các hàm tiện ích dùng chung (đọc/ghi parquet, tạo spark session).
├── shell/                          # Bash scripts được gọi bởi Airflow để submit các Job Spark.
│   └── hospital_etl_airflow.sh
├── Dockerfile.airflow              # File Docker tùy chỉnh cho Airflow (Đã tích hợp sẵn Docker CLI).
├── docker-compose.yml              # Cấu hình kiến trúc hạ tầng (HDFS, Spark, Airflow, MySQL, Postgres).
├── generate_source_data.py         # Script Python độc lập dùng để đổ dữ liệu mẫu vào MySQL (Cách thay thế/Dự phòng).
└── requirements.txt                # Danh sách thư viện Python cần thiết (pymysql, pyspark...).
```

---

## 🛠 Yêu cầu hệ thống (Requirements)
*   **Docker** & **Docker Compose** (Dùng để dựng môi trường Cluster).
*   **Môi trường mạng** ổn định có thể kết nối tới máy chủ ClickHouse Cloud.
*   Môi trường mạng có thể kết nối tới ClickHouse Cloud (hoặc tự cấu hình ClickHouse local).

---

## 🚀 Hướng dẫn Cài đặt & Chạy dự án

### Bước 1: Khởi động hệ thống (Build & Run)
Toàn bộ hạ tầng (HDFS NameNode/DataNode, Spark Master/Worker, Airflow Webserver/Scheduler, MySQL, Postgres) đã được đóng gói bằng Docker Compose.

Mở Terminal tại thư mục `Hospital_ETL` và chạy:
```bash
docker compose up --build -d
```
*Lưu ý:* Cờ `--build` ở lần đầu tiên sẽ giúp Docker tự động đọc file `Dockerfile.airflow` để cài sẵn Docker CLI vào Airflow, giúp giao diện UI bật lên siêu nhanh (dưới 5s ở những lần chạy sau).

### Bước 2: Tạo Dữ liệu Nguồn (Source Data) cho MySQL
Hệ thống cần có dữ liệu trong MySQL để bắt đầu quá trình ETL. Rất may, với thiết lập hiện tại, dữ liệu nguồn sẽ được **tự động khởi tạo** ngay khi bạn chạy lệnh `docker compose up` lần đầu tiên!

Cụ thể, thư mục `datasource/` chứa các file `.sql` đã được map thẳng vào thư mục `/docker-entrypoint-initdb.d/` bên trong container MySQL. Tiến trình nội bộ của MySQL sẽ tự động duyệt qua các file này và đổ dữ liệu vào DB. Bạn **không cần phải cài đặt Python hay chạy bất kỳ câu lệnh nào cả**!

*(Lưu ý nhỏ: Cơ chế này chỉ chạy duy nhất 1 lần khi ổ cứng ảo `mysql_data` đang trống rỗng. Nếu bạn muốn xóa toàn bộ data hiện tại và sinh lại dữ liệu từ đầu, hãy gõ lệnh `docker compose down -v` để xóa Volume trước khi up lại).*

### Bước 3: Theo dõi và Chạy luồng ETL qua Airflow
1. Mở trình duyệt, truy cập vào giao diện Airflow:
   👉 **http://localhost:8082**
2. Đăng nhập với tài khoản:
   - **Username:** `admin`
   - **Password:** `admin`
3. Tại giao diện chính, bật On cho DAG có tên `hospital_etl_dag`.
4. Bấm nút Play (Trigger DAG) để kích hoạt luồng chạy. Tiến trình sẽ lần lượt đi qua các task:
   `extract_mysql_to_hdfs` ➔ `build_staging` ➔ `build_dimensions` ➔ `build_facts` ➔ `load_hdfs_to_clickhouse_dw`.
5. *Quản lý file trên HDFS:* Xem trực tiếp dữ liệu thô và sạch tại HDFS Web UI: **http://localhost:9870**.

---

## ⚙️ Cấu hình (Configuration)
Tất cả các tham số kết nối hệ thống (Database password, ClickHouse cloud connection, thư mục HDFS) được quy định trong:
1. File `.env` (Nếu có).
2. Mã nguồn trong file `hospital_utils.py` và đầu vào của file `load_hdfs_to_clickhouse_dw.py` (Cấu hình ClickHouse).
Nếu muốn thay đổi thông tin kết nối ClickHouse, hãy chỉnh sửa biến môi trường (Environment variables) như `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, v.v.
