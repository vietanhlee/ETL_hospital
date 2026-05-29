"""
DAG điều phối luồng ETL Bệnh viện (Hospital ETL Pipeline).
Sử dụng Airflow để tự động hóa việc chạy các job Spark qua BashOperator.
Thứ tự thực thi: Extract (MySQL -> HDFS) -> Staging (Silver) -> Dimensions (Gold) -> Facts (Gold) -> Load (ClickHouse).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# Cấu hình mặc định cho các task trong DAG
default_args = {
    "owner": "hospital_etl_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=3), # Nếu fail, đợi 3 phút rồi chạy lại
}


with DAG(
    dag_id="hospital_etl_dag",
    default_args=default_args,
    description="Hospital ETL: MySQL source -> HDFS raw -> staging -> warehouse -> ClickHouse DW",
    start_date=datetime(2026, 5, 12),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["hospital", "etl", "spark", "hdfs", "mysql"],
) as dag:

    # ---------------------------------------------------------
    # Tham số cấu hình chung:
    # Lấy ngày chạy (run_date) từ tham số trigger (conf) hoặc dùng mặc định ngày của DAG (ds)
    # ds: là execution_date format YYYY-MM-DD
    # ---------------------------------------------------------
    run_date = "{{ dag_run.conf.get('run_date', ds) }}"

    extract_mysql_to_hdfs = BashOperator(
        task_id="extract_mysql_to_hdfs",
        bash_command=f"""
        bash /opt/spark-apps/hospital_etl/shell/hospital_etl_airflow.sh extract {run_date}
        """,
    )

    build_staging = BashOperator(
        task_id="build_staging",
        bash_command=f"""
        bash /opt/spark-apps/hospital_etl/shell/hospital_etl_airflow.sh staging {run_date}
        """,
    )

    build_dimensions = BashOperator(
        task_id="build_dimensions",
        bash_command=f"""
        bash /opt/spark-apps/hospital_etl/shell/hospital_etl_airflow.sh dimension {run_date}
        """,
    )

    build_facts = BashOperator(
        task_id="build_facts",
        bash_command=f"""
        bash /opt/spark-apps/hospital_etl/shell/hospital_etl_airflow.sh fact {run_date}
        """,
    )

    load_hdfs_to_clickhouse_dw = BashOperator(
        task_id="load_hdfs_to_clickhouse_dw",
        bash_command=f"""
        bash /opt/spark-apps/hospital_etl/shell/hospital_etl_airflow.sh load {run_date}
        """,
    )

    # Task MLOps: Dự báo doanh thu
    train_ml_revenue = BashOperator(
        task_id="train_ml_revenue_forecast",
        bash_command="export MLFLOW_TRACKING_URI='http://mlflow-server:5000' && python /opt/spark-apps/hospital_etl/scripts_final/train_revenue_forecast.py",
    )

    # ---------------------------------------------------------
    # Xác định thứ tự chạy các task (Dependencies)
    # Data Lake Medallion Architecture: Bronze -> Silver -> Gold -> Data Warehouse
    # ---------------------------------------------------------
    extract_mysql_to_hdfs >> build_staging >> build_dimensions >> build_facts >> load_hdfs_to_clickhouse_dw >> train_ml_revenue