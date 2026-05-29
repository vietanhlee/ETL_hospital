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


def read_csv(spark, path: str):
    """
    Đọc dữ liệu từ file CSV trên HDFS vào Spark DataFrame.
    
    Args:
        spark (SparkSession): Đối tượng SparkSession hiện tại.
        path (str): Đường dẫn tới thư mục/file CSV trên HDFS.
        
    Returns:
        DataFrame: Dữ liệu DataFrame đã được đọc lên (tất cả các cột đều là String).
    """
    return spark.read.option("header", True) \
        .option("inferSchema", False) \
        .option("encoding", "UTF-8") \
        .csv(path)


def write_csv(df, path: str):
    """
    Ghi dữ liệu Spark DataFrame ra HDFS dưới dạng 1 file CSV.
    
    Args:
        df (DataFrame): Dữ liệu cần ghi.
        path (str): Đường dẫn đích trên HDFS.
    """
    df.coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .option("encoding", "UTF-8") \
        .csv(path)


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


def write_mysql_table(df, jdbc_url: str, table_name: str, user: str, password: str, mode: str = "append"):
    """
    Ghi Spark DataFrame xuống bảng MySQL qua JDBC.
    
    Args:
        df (DataFrame): Dữ liệu cần ghi.
        jdbc_url (str): Chuỗi kết nối JDBC.
        table_name (str): Tên bảng MySQL đích.
        user (str): Tên đăng nhập DB.
        password (str): Mật khẩu DB.
        mode (str, optional): Chế độ ghi ("append", "overwrite"). Mặc định là "append".
    """
    (df.write.format("jdbc")
       .option("url", jdbc_url)
       .option("dbtable", table_name)
       .option("user", user)
       .option("password", password)
       .option("driver", "com.mysql.cj.jdbc.Driver")
       .option("batchsize", "5000")
       .mode(mode)
       .save())


def execute_mysql_sql(spark, jdbc_url: str, user: str, password: str, sql_text: str):
    """
    Thực thi mã lệnh SQL thuần (DDL/DML) trên MySQL thông qua Spark JVM (Py4J).
    
    Args:
        spark (SparkSession): Đối tượng SparkSession.
        jdbc_url (str): Chuỗi kết nối JDBC.
        user (str): Tên đăng nhập DB.
        password (str): Mật khẩu DB.
        sql_text (str): Mã SQL (có thể gồm nhiều câu lệnh phân tách bởi dấu ;).
    """
    jvm = spark.sparkContext._gateway.jvm
    conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    stmt = conn.createStatement()
    try:
        for sql in sql_text.split(";"):
            sql = sql.strip()
            if sql:
                stmt.execute(sql)
    finally:
        stmt.close()
        conn.close()
