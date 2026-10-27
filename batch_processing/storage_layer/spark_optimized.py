"""Đọc CSV bằng PySpark SQL và nạp các bảng DWH vào PostgreSQL."""

import argparse
import os
import re
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T


JDBC_JAR = Path(__file__).resolve().parents[2] / "drive_needed" / "postgresql-42.7.14.jar"


def local_classpath() -> str:
    """use JAR local not to let Windows call winutils via spark.jars/packages."""
    ivy = Path.home() / ".ivy2" / "jars"
    jars = [JDBC_JAR]
    for pattern in (
        "org.apache.hadoop_hadoop-aws-*.jar",
        "com.amazonaws_aws-java-sdk-bundle-*.jar",
        "org.wildfly.openssl_wildfly-openssl-*.jar",
    ):
        jars.extend(ivy.glob(pattern))
    return os.pathsep.join(str(path) for path in jars)


SCHEMA = T.StructType([
    T.StructField("order_id", T.LongType()),
    T.StructField("user_id", T.LongType()),
    T.StructField("order_number", T.IntegerType()),
    T.StructField("order_dow", T.ShortType()),
    T.StructField("order_hour_of_day", T.ShortType()),
    T.StructField("days_since_prior_order", T.DoubleType()),
    T.StructField("product_id", T.LongType()),
    T.StructField("product_name", T.StringType()),
    T.StructField("aisle_id", T.IntegerType()),
    T.StructField("department_id", T.ShortType()),
    T.StructField("category", T.StringType()),
    T.StructField("add_to_cart_order", T.ShortType()),
    T.StructField("reordered", T.ShortType()),
    T.StructField("city", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("order_month", T.StringType()),
    T.StructField("schema_version", T.ShortType()),
    T.StructField("coupon_code", T.StringType()),       # NULL at schema v1
    T.StructField("shipping_method", T.StringType()),  # NULL at schema v1
])


def make_spark() -> SparkSession:
    if not JDBC_JAR.exists():
        raise FileNotFoundError(f"Missing PostgreSQL JDBC driver: {JDBC_JAR}")
    # Có hiệu lực khi chạy trực tiếp bằng python; spark-submit vẫn có thể override.
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--driver-memory {os.getenv('SPARK_DRIVER_MEMORY', '3g')} pyspark-shell",
    )
    classpath = local_classpath()
    return (
        SparkSession.builder
        # local[*] tạo quá nhiều task đồng thời và dễ làm JVM hết RAM trên Windows.
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .appName("instacart-to-postgres-dwh")
        .config("spark.driver.extraClassPath", classpath)
        .config("spark.executor.extraClassPath", classpath)
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Ho_Chi_Minh")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Ho_Chi_Minh")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.adaptive.enabled", "true")
        #AQE
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_PARTITIONS", "8"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "seaweedfs"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "seaweedfs"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.ui.port","3636") #spark ui   
        .getOrCreate()
    )


def read_clean(spark: SparkSession, source: str):
    raw = spark.read.option("header", True).schema(SCHEMA).csv(source)
    month = F.to_date(F.concat(F.trim("order_month"), F.lit("-01")), "yyyy-MM-dd")
    is_valid = (
        (F.col("order_id") > 0)
        & (F.col("user_id") > 0)
        & (F.col("product_id") > 0)
        & month.isNotNull()
        & (F.col("amount") >= 0)
    )
    counts = raw.agg(
        F.count("*").alias("raw"),
        F.sum(F.when(is_valid, 1).otherwise(0)).alias("valid"),
    ).first()
    clean = (
        raw.filter(is_valid).dropDuplicates(["order_id"])
        .withColumn("order_month", month)
        .withColumn("city", F.coalesce("city", F.lit("UNKNOWN")))
        .withColumn("product_name", F.coalesce("product_name", F.lit("UNKNOWN")))
        .withColumn("category", F.coalesce("category", F.lit("UNKNOWN")))
        .withColumn("coupon_code", F.coalesce("coupon_code", F.lit("UNKNOWN")))
        .withColumn("shipping_method", F.coalesce("shipping_method", F.lit("UNKNOWN")))
        .withColumn("ingested_at", F.current_timestamp())
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    return clean, int(counts["raw"]), int(counts["valid"] or 0)


def make_tables(spark: SparkSession, clean, raw_count: int, valid_count: int):
    dim_product = clean.select(
        "product_id", "product_name", "aisle_id", "department_id", "category"
    ).dropDuplicates(["product_id"])
    fact_orders = clean.drop(
        "product_name", "aisle_id", "department_id", "category"
    )

    summary = clean.groupBy("city", "order_month").agg(
        F.count("*").alias("order_count"),
        F.countDistinct("user_id").alias("unique_users"),
        F.sum("amount").alias("total_amount"),
    )

    clean_count = clean.count()  # materialize cache một lần cho các bảng sau
    # Dùng Spark SQL thuần JVM; createDataFrame từ list Python sẽ khởi động
    # Python worker không cần thiết chỉ để tạo bốn dòng metrics.
    quality = spark.sql(f"""
        SELECT metric_name, metric_value, current_timestamp() AS measured_at
        FROM VALUES
            ('raw_rows', CAST({raw_count} AS BIGINT)),
            ('invalid_rows', CAST({raw_count - valid_count} AS BIGINT)),
            ('duplicate_rows_removed', CAST({valid_count - clean_count} AS BIGINT)),
            ('clean_rows', CAST({clean_count} AS BIGINT))
        AS metrics(metric_name, metric_value)
    """)

    return {
        "dim_product": dim_product,
        "fact_orders": fact_orders,
        "city_month_summary": summary,
        "pipeline_quality_metrics": quality,
    }


def postgres_connection(spark: SparkSession, url: str, user: str, password: str):
    """Mở JDBC connection bằng đúng classloader mà Spark đã nạp driver."""
    jvm = spark.sparkContext._gateway.jvm
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    driver = loader.loadClass("org.postgresql.Driver").newInstance()
    properties = jvm.java.util.Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    return driver.connect(url, properties)


def ensure_schema(spark: SparkSession, url: str, user: str, password: str, schema: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError(f"Invalid PostgreSQL schema name: {schema!r}")
    connection = postgres_connection(spark, url, user, password)
    try:
        statement = connection.createStatement()
        try:
            statement.executeUpdate(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        finally:
            statement.close()
    finally:
        connection.close()


def write_postgres(spark: SparkSession, tables: dict) -> None:
    url = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/metastore")
    schema = os.getenv("POSTGRES_SCHEMA", "warehouse")
    user = os.getenv("POSTGRES_USER", "hive")
    password = os.getenv("POSTGRES_PASSWORD", "hive")
    ensure_schema(spark, url, user, password, schema)
    options = {
        "url": url + ("&" if "?" in url else "?") + "reWriteBatchedInserts=true",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        # Batch vừa phải: nhanh hơn insert từng dòng nhưng không làm đầy heap.
        "batchsize": os.getenv("JDBC_BATCH_SIZE", "1000"),
        "isolationLevel": "READ_COMMITTED",
        "truncate": "true",
    }
    for name, frame in tables.items():
        (
            frame.write.format("jdbc")
            .options(**options, dbtable=f"{schema}.{name}")
            .mode("overwrite")
            .save()
        )
        print(f"Loaded {schema}.{name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="s3a://department-data/raw/instacart/*.csv")
    args = parser.parse_args()

    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")
    clean = None
    try:
        clean, raw_count, valid_count = read_clean(spark, args.source)
        write_postgres(spark, make_tables(spark, clean, raw_count, valid_count))
    except Exception:
        print(
            "Spark job failed. If the Java process disappeared, close other apps "
            "or reduce SPARK_MASTER before retrying."
        )
        raise
    finally:
        if clean is not None:
            try:
                clean.unpersist(blocking=False)
            except Exception:
                pass  # JVM may already have terminated; preserve the original error.
        try:
            if not spark.sparkContext._jsc.sc().isStopped():
                spark.stop()
        except Exception:
            pass  # Do not mask the actual Spark/JVM failure with a stop() error.


if __name__ == "__main__":
    input("Press Enter after inspecting Spark UI...")
    main()
