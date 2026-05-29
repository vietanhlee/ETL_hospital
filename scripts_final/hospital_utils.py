# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession


def create_spark(app_name: str):
    """
    Khởi tạo và cấu hình SparkSession kết nối tới Spark Master.
    
    Args:
        app_name (str): Tên của ứng dụng (Job Name) hiển thị trên Spark UI.
        
    Returns:
        SparkSession: Đối tượng SparkSession đã được cấu hình.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def read_parquet(spark, path: str):
    """
    Đọc dữ liệu từ file Parquet trên HDFS vào Spark DataFrame.
    
    Args:
        spark (SparkSession): Đối tượng SparkSession hiện tại.
        path (str): Đường dẫn tới thư mục/file Parquet trên HDFS.
        
    Returns:
        DataFrame: Dữ liệu DataFrame giữ nguyên được schema gốc.
    """
    return spark.read.parquet(path)


def write_parquet(df, path: str):
    """
    Ghi dữ liệu Spark DataFrame ra HDFS dưới định dạng Parquet.
    (Chế độ ghi đè - Overwrite).
    
    Args:
        df (DataFrame): Dữ liệu cần ghi.
        path (str): Đường dẫn đích trên HDFS.
    """
    df.write.mode("overwrite").parquet(path)


def write_error(df, path: str):
    """
    Ghi dữ liệu rác/lỗi (Quarantine) ra HDFS dưới định dạng Parquet.
    
    Args:
        df (DataFrame): Dữ liệu chứa các bản ghi lỗi/mồ côi (Orphan records).
        path (str): Đường dẫn thư mục cách ly trên HDFS.
    """
    # Error records are written as Parquet to preserve schema and Vietnamese text safely.
    df.write.mode("overwrite").parquet(path)


def read_mysql_table(spark, jdbc_url: str, table_name: str, user: str, password: str):
    """
    Đọc toàn bộ bảng từ cơ sở dữ liệu MySQL thông qua JDBC.
    
    Args:
        spark (SparkSession): Đối tượng SparkSession.
        jdbc_url (str): Chuỗi kết nối JDBC (VD: jdbc:mysql://host:port/db).
        table_name (str): Tên bảng MySQL cần đọc.
        user (str): Tên đăng nhập DB.
        password (str): Mật khẩu DB.
        
    Returns:
        DataFrame: Dữ liệu được kéo từ MySQL.
    """
    return (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", user)
            .option("password", password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load())


def get_jdbc_url(db_name=""):
    """
    Tạo chuỗi kết nối JDBC tới ClickHouse Cloud.
    
    Args:
        db_name (str): Tên database. Mặc định là chuỗi rỗng.
        
    Returns:
        str: Chuỗi kết nối JDBC hoàn chỉnh.
    """
    host = os.getenv('CLICKHOUSE_HOST', 'qlfb8ypu5w.ap-northeast-1.aws.clickhouse.cloud')
    port = os.getenv('CLICKHOUSE_PORT', '8443')
    secure = os.getenv('CLICKHOUSE_SECURE', 'True').lower() in ('true', '1', 't')
    
    url = f"jdbc:clickhouse://{host}:{port}/{db_name}"
    if secure:
        url += "?ssl=true"
    return url


def execute_ch_sql_http(host, port, user, password, db_name, sql_text, secure):
    """
    Thực thi mã SQL (DDL/DML) trên ClickHouse thông qua HTTP/HTTPS API.
    Sử dụng để tạo Database và tạo Bảng (CREATE TABLE) vì JDBC của Spark 
    không hỗ trợ tốt các lệnh DDL phức tạp của ClickHouse.
    
    Args:
        host, port, user, password (str): Thông tin kết nối ClickHouse.
        db_name (str): Tên database.
        sql_text (str): Mã SQL cần thực thi.
        secure (bool): Dùng HTTPS (True) hay HTTP (False).
    """
    protocol = "https" if secure else "http"
    url = f"{protocol}://{host}:{port}/"
    if db_name:
        url += f"?database={db_name}"
        
    auth_str = f"{user}:{password}"
    b64_auth = base64.b64encode(auth_str.encode('ascii')).decode('ascii')
    
    for sql in sql_text.split(";"):
        sql = sql.strip()
        if sql:
            req = urllib.request.Request(url, data=sql.encode('utf-8'))
            req.add_header("Authorization", f"Basic {b64_auth}")
            try:
                with urllib.request.urlopen(req) as response:
                    response.read()
            except Exception as e:
                print(f"Error executing SQL: {sql}")
                raise e


def load_to_clickhouse(df, table_name, db_name='hospital_dw'):
    """
    Đẩy Spark DataFrame lên bảng ClickHouse qua giao thức JDBC.
    Dữ liệu được đẩy theo từng lô (batchsize=10000) để tối ưu hiệu suất.
    
    Args:
        df (DataFrame): Dữ liệu cần nạp.
        table_name (str): Tên bảng đích trên ClickHouse.
        db_name (str): Tên database đích.
    """
    jdbc_url = get_jdbc_url(db_name)
    user = os.getenv('CLICKHOUSE_USER', 'default')
    password = os.getenv('CLICKHOUSE_PASSWORD', 'N7f8bLl.qrbON')
    
    print(f"Inserting rows into {db_name}.{table_name} via JDBC...")
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
        .option("batchsize", "10000") \
        .mode("append") \
        .save()
    print(f"Insert into {table_name} completed.")

