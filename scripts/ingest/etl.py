from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DateType,
)

RAW_DIR = Path("raw_data")
OUTPUT_DIR = Path("hdfs_output") / "ingested"
WAREHOUSE_DIR = Path("spark_warehouse").resolve()
SCRATCH_DIR = Path("/tmp/spark_scratch").resolve()

SCHEMAS = {
    "raw_clickstream": StructType([
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("device_type", StringType(), False),
        StructField("location_city", StringType(), False),
        StructField("page_url", StringType(), False),
        StructField("referrer_source", StringType(), False),
    ]),
    "raw_customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("phone_number", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("signup_date", DateType(), False),
        StructField("customer_type", StringType(), False),
        StructField("is_active", StringType(), False),
    ]),
    "raw_inventory": StructType([
        StructField("inventory_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("location_type", StringType(), False),
        StructField("location_id", StringType(), False),
        StructField("quantity_available", IntegerType(), False),
        StructField("reorder_level", IntegerType(), False),
        StructField("last_updated", TimestampType(), False),
    ]),
    "raw_order_items": StructType([
        StructField("order_item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("item_price", DoubleType(), False),
        StructField("discount_amount", DoubleType(), False),
        StructField("line_total", DoubleType(), False),
    ]),
    "raw_orders": StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_channel", StringType(), False),
        StructField("store_id", StringType(), True),
        StructField("order_source", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField("order_timestamp", TimestampType(), False),
        StructField("order_status", StringType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("payment_id", StringType(), False),
    ]),
    "raw_payments": StructType([
        StructField("payment_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("payment_status", StringType(), False),
        StructField("failure_reason", StringType(), True),
        StructField("payment_timestamp", TimestampType(), False),
        StructField("amount_paid", DoubleType(), False),
    ]),
    "raw_products": StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("sub_category", StringType(), False),
        StructField("brand", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("cost_price", DoubleType(), False),
        StructField("launch_date", DateType(), False),
        StructField("is_active", StringType(), False),
    ]),
    "raw_stores": StructType([
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("region", StringType(), False),
        StructField("opening_date", DateType(), False),
        StructField("store_type", StringType(), False),
    ]),
}

CSV_FILES = {
    "raw_clickstream": RAW_DIR / "clickstream.csv",
    "raw_customers": RAW_DIR / "customers.csv",
    "raw_inventory": RAW_DIR / "inventory.csv",
    "raw_order_items": RAW_DIR / "order_items.csv",
    "raw_orders": RAW_DIR / "orders.csv",
    "raw_payments": RAW_DIR / "payments.csv",
    "raw_products": RAW_DIR / "products.csv",
    "raw_stores": RAW_DIR / "stores.csv",
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("retail_capstone_etl")
        .master("local[1]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR))
        .config("spark.hadoop.hive.metastore.warehouse.dir", str(WAREHOUSE_DIR))
        .config("spark.hadoop.hive.exec.scratchdir", str(SCRATCH_DIR))
        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .config("spark.hadoop.hadoop.security.authorization", "false")
        .config("spark.hadoop.javax.security.auth.useSubjectCredsOnly", "false")
        .enableHiveSupport()
        .getOrCreate()
    )


def read_csv_table(spark: SparkSession, table_name: str):
    csv_path = CSV_FILES[table_name]
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing raw CSV file: {csv_path}")

    schema = SCHEMAS[table_name]
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(str(csv_path))
    )


def write_table(df, table_name: str):
    output_path = OUTPUT_DIR / table_name
    print(f"Writing Hive table '{table_name}' and parquet output to {output_path}")
    df.write.mode("overwrite").saveAsTable(table_name)
    df.write.mode("overwrite").parquet(str(output_path))


if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    print("Spark session created with warehouse:", WAREHOUSE_DIR)

    for table_name in CSV_FILES:
        df = read_csv_table(spark, table_name)
        print(f"Read {df.count()} rows from {CSV_FILES[table_name].name}")
        df.printSchema()
        write_table(df, table_name)

    print("Hive tables created:")
    spark.sql("SHOW TABLES").show(truncate=False)

    print("Sample data from raw_orders:")
    spark.table("raw_orders").show(5, truncate=False)

    spark.stop()
    print("ETL complete.")
