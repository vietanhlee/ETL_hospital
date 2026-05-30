#!/bin/bash

set -e
# ==========================================================
# Usage:
#   bash hospital_etl_airflow.sh <job_name> <run_date>
#
# Examples:
#   bash hospital_etl_airflow.sh extract 2026-05-12
#   bash hospital_etl_airflow.sh staging 2026-05-12
#   bash hospital_etl_airflow.sh dimension 2026-05-12
#   bash hospital_etl_airflow.sh fact 2026-05-12
#   bash hospital_etl_airflow.sh load 2026-05-12
#   bash hospital_etl_airflow.sh all 2026-05-12
#
# JOB_NAME allowed values:
#   extract | staging | dimension | fact | load | all
# ==========================================================

JOB_NAME=$1
RUN_DATE=$2

if [ -z "$JOB_NAME" ]; then
    echo "ERROR: Missing JOB_NAME"
    echo "Usage: bash hospital_etl_airflow.sh <job_name> <run_date>"
    exit 1
fi

if [ -z "$RUN_DATE" ]; then
    echo "ERROR: Missing RUN_DATE"
    echo "Usage: bash hospital_etl_airflow.sh <job_name> <run_date>"
    exit 1
fi

PROJECT_DIR="/opt/spark-apps/hospital_etl"

# Nếu muốn chạy bản PySpark API thì đổi scripts_sql thành scripts.
# hiện tại đang chạy bản final - SQL thuần
SCRIPTS_DIR="${PROJECT_DIR}/scripts_final"

SPARK_MASTER_CONTAINER="spark-master"
SPARK_MASTER_URL="spark://spark-master:7077"

SPARK_EXECUTOR_CORES="2"
SPARK_CORES_MAX="4"
SPARK_EXECUTOR_MEMORY="1536m"


echo "=================================================="
echo "Hospital ETL Airflow Runner - 4 layers"
echo "JOB_NAME         = ${JOB_NAME}"
echo "RUN_DATE         = ${RUN_DATE}"
echo "PROJECT_DIR      = ${PROJECT_DIR}"
echo "SCRIPTS_DIR      = ${SCRIPTS_DIR}"
echo "SPARK_MASTER_URL = ${SPARK_MASTER_URL}"
echo "HOSTNAME         = $(hostname)"
echo "HOST_IP          = $(hostname -i 2>/dev/null || echo unknown)"
echo "=================================================="


# ---------------------------------------------------------
# Hàm run_spark_directly_inside_container
# Chạy trực tiếp lệnh spark-submit nếu môi trường hiện tại
# (ví dụ container có sẵn Spark) hỗ trợ.
# ---------------------------------------------------------
run_spark_directly_inside_container() {
    PY_FILE=$1
    PY_PATH="${SCRIPTS_DIR}/${PY_FILE}"

    DRIVER_HOST=$(hostname -i | awk '{print $1}')

    echo "--------------------------------------------------"
    echo "Mode       : direct spark-submit inside current container"
    echo "Driver host: ${DRIVER_HOST}"
    echo "Python file: ${PY_PATH}"
    echo "Run date   : ${RUN_DATE}"
    echo "--------------------------------------------------"

    if [ ! -f "${PY_PATH}" ]; then
        echo "ERROR: Python file not found: ${PY_PATH}"
        exit 1
    fi

    if [ "${PY_FILE}" == "load_hdfs_to_clickhouse_dw.py" ]; then
        echo "Using ClickHouse JDBC driver for ClickHouse load..."
    fi

    spark-submit \
      --packages mysql:mysql-connector-java:8.0.33,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2 \
      --master ${SPARK_MASTER_URL} \
      --conf spark.driver.host=${DRIVER_HOST} \
      --conf spark.driver.bindAddress=0.0.0.0 \
      --conf spark.executor.cores=${SPARK_EXECUTOR_CORES} \
      --conf spark.cores.max=${SPARK_CORES_MAX} \
      --conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY} \
      --conf spark.sql.legacy.timeParserPolicy=LEGACY \
      ${PY_PATH} \
      ${RUN_DATE}
}


# ---------------------------------------------------------
# Hàm run_spark_from_host_by_docker_exec
# Chạy gián tiếp bằng cách gửi lệnh qua 'docker exec' sang
# container 'spark-master' (dành cho Airflow Container).
# ---------------------------------------------------------
run_spark_from_host_by_docker_exec() {
    PY_FILE=$1
    PY_PATH="${SCRIPTS_DIR}/${PY_FILE}"

    echo "--------------------------------------------------"
    echo "Mode       : docker exec spark-master spark-submit"
    echo "Python file: ${PY_PATH}"
    echo "Run date   : ${RUN_DATE}"
    echo "--------------------------------------------------"

    docker exec ${SPARK_MASTER_CONTAINER} bash -lc "test -f ${PY_PATH}"
    if [ $? -ne 0 ]; then
        echo "ERROR: Python file not found inside ${SPARK_MASTER_CONTAINER}: ${PY_PATH}"
        exit 1
    fi

    if [ "${PY_FILE}" == "load_hdfs_to_clickhouse_dw.py" ]; then
        echo "Using ClickHouse JDBC driver for ClickHouse load in ${SPARK_MASTER_CONTAINER}..."
    fi

    docker exec ${SPARK_MASTER_CONTAINER} /spark/bin/spark-submit \
      --packages mysql:mysql-connector-java:8.0.33,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2 \
      --master ${SPARK_MASTER_URL} \
      --conf spark.driver.host=spark-master \
      --conf spark.driver.bindAddress=0.0.0.0 \
      --conf spark.executor.cores=${SPARK_EXECUTOR_CORES} \
      --conf spark.cores.max=${SPARK_CORES_MAX} \
      --conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY} \
      --conf spark.sql.legacy.timeParserPolicy=LEGACY \
      ${PY_PATH} \
      ${RUN_DATE}
}


# ---------------------------------------------------------
# Hàm run_spark_job
# Hàm định tuyến: Tự động kiểm tra môi trường hiện tại để quyết
# định cách gọi lệnh spark-submit cho phù hợp.
# ---------------------------------------------------------
run_spark_job() {
    PY_FILE=$1

    if [ -f /.dockerenv ] && command -v spark-submit >/dev/null 2>&1; then
        run_spark_directly_inside_container "${PY_FILE}"

    elif command -v docker >/dev/null 2>&1; then
        run_spark_from_host_by_docker_exec "${PY_FILE}"

    elif command -v spark-submit >/dev/null 2>&1; then
        run_spark_directly_inside_container "${PY_FILE}"

    else
        echo "ERROR: Cannot find spark-submit or docker."
        exit 1
    fi
}


case ${JOB_NAME} in

    extract)
        run_spark_job "extract_mysql_to_hdfs.py"
        ;;

    staging)
        run_spark_job "hospital_staging_job.py"
        ;;

    dimension)
        run_spark_job "hospital_dimension_job.py"
        ;;

    fact)
        run_spark_job "hospital_fact_job.py"
        ;;

    load)
        run_spark_job "load_hdfs_to_clickhouse_dw.py"
        ;;

    all)
        run_spark_job "extract_mysql_to_hdfs.py"
        run_spark_job "hospital_staging_job.py"
        run_spark_job "hospital_dimension_job.py"
        run_spark_job "hospital_fact_job.py"
        run_spark_job "load_hdfs_to_clickhouse_dw.py"
        ;;

    *)
        echo "ERROR: Invalid JOB_NAME: ${JOB_NAME}"
        echo "Allowed values: extract | staging | dimension | fact | load | all"
        exit 1
        ;;
esac


echo "=================================================="
echo "Done job: ${JOB_NAME}"
echo "Run date: ${RUN_DATE}"
echo "=================================================="
