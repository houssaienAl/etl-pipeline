"""PySpark job: clean + join the three sources and load PostgreSQL."""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ORDERS_SCHEMA = "order_id BIGINT, customer_id BIGINT, order_ts TIMESTAMP, currency STRING, amount DOUBLE, status STRING"
CUSTOMERS_SCHEMA = "customer_id BIGINT, name STRING, country STRING, signup_date STRING"
FX_SCHEMA = "currency STRING, rate_to_eur DOUBLE"
VALID_STATUSES = ["completed", "pending", "cancelled", "refunded"]


def get_spark():
    return (
        SparkSession.builder
        # local[*] by default; point SPARK_MASTER at spark://host:7077 to use a cluster
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .appName("etl_pipeline")
        .config("spark.jars", os.getenv("PG_JDBC_JAR", "/opt/jars/postgresql.jar"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def clean_customers(customers):
    return (
        customers.dropna(subset=["customer_id"])
        .dropDuplicates(["customer_id"])
        .select(
            "customer_id",
            F.trim("name").alias("name"),
            F.upper(F.trim("country")).alias("country"),
            F.to_date("signup_date").alias("signup_date"),
        )
    )


def clean_orders(orders, customers, fx):
    """Drop invalid rows, dedupe, keep only known customers, convert to EUR."""
    return (
        orders.withColumn("status", F.lower(F.trim("status")))
        .withColumn("currency", F.upper(F.trim("currency")))
        .dropna(subset=["order_id", "customer_id", "order_ts", "amount"])
        .filter((F.col("amount") > 0) & F.col("status").isin(VALID_STATUSES))
        .dropDuplicates(["order_id"])
        .join(customers.select("customer_id"), "customer_id", "left_semi")
        .join(F.broadcast(fx), "currency")  # inner: unknown currencies are dropped
        .withColumn("amount_eur", F.round(F.col("amount") * F.col("rate_to_eur"), 2))
        .select("order_id", "customer_id", "order_ts", "currency", "amount", "amount_eur", "status")
    )


def write_table(df, table):
    url = (
        f"jdbc:postgresql://{os.getenv('WAREHOUSE_HOST', 'localhost')}:5432/"
        f"{os.getenv('WAREHOUSE_DB', 'warehouse')}?reWriteBatchedInserts=true"
    )
    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", os.getenv("WAREHOUSE_USER", "etl"))
        .option("password", os.getenv("WAREHOUSE_PASSWORD", "etl"))
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 50_000)
        .option("truncate", "true")  # keep DDL/PKs, full reload each run (idempotent)
        .mode("overwrite")
        .save()
    )


def run(data_dir=None):
    data_dir = data_dir or os.getenv("DATA_DIR", "data")
    spark = get_spark()
    try:
        orders = spark.read.csv(f"{data_dir}/orders.csv", header=True, schema=ORDERS_SCHEMA)
        customers = clean_customers(spark.read.json(f"{data_dir}/customers.json", schema=CUSTOMERS_SCHEMA))
        fx = spark.read.json(f"{data_dir}/fx_rates.json", schema=FX_SCHEMA)

        raw_count = orders.count()
        clean = clean_orders(orders, customers, fx).cache()
        clean_count = clean.count()
        write_table(customers, "dim_customers")
        write_table(clean, "fact_orders")
        print(f"orders: {raw_count} raw rows -> {clean_count} loaded ({raw_count - clean_count} rejected)")
        return {"raw": raw_count, "loaded": clean_count}
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
