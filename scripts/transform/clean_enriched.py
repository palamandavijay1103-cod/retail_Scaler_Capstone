from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    to_date,
    year,
    month,
    dayofmonth,
    quarter,
    dayofweek,
    when,
)

OUTPUT_DIR = Path("hdfs_output") / "cleaned"
WAREHOUSE_DIR = Path("spark_warehouse").resolve()
SCRATCH_DIR = Path("/tmp/spark_scratch").resolve()

RAW_TABLES = [
    "raw_clickstream",
    "raw_customers",
    "raw_inventory",
    "raw_order_items",
    "raw_orders",
    "raw_payments",
    "raw_products",
    "raw_stores",
]


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("retail_capstone_transform")
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


def validate_raw_tables(spark: SparkSession):
    missing = [t for t in RAW_TABLES if not spark.catalog.tableExists(t)]
    if missing:
        raise RuntimeError(
            "Missing raw Hive tables. Run the ingestion ETL before transform: "
            + ", ".join(missing)
        )


def write_table(df, table_name: str):
    output_path = OUTPUT_DIR / table_name
    print(f"Writing cleaned table '{table_name}' and parquet output to {output_path}")
    df.write.mode("overwrite").saveAsTable(table_name)
    df.write.mode("overwrite").parquet(str(output_path))


def clean_customers(df):
    return (
        df.select(
            col("customer_id"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            lower(trim(col("email"))).alias("email"),
            trim(col("phone_number")).alias("phone_number"),
            trim(col("city")).alias("city"),
            trim(col("state")).alias("state"),
            col("signup_date"),
            trim(col("customer_type")).alias("customer_type"),
            when(lower(trim(col("is_active"))) == "y", True).otherwise(False).alias("is_active"),
        )
        .dropDuplicates(["customer_id"])
    )


def clean_products(df):
    return (
        df.select(
            col("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("sub_category")).alias("sub_category"),
            trim(col("brand")).alias("brand"),
            col("price"),
            col("cost_price"),
            col("launch_date"),
            when(lower(trim(col("is_active"))) == "y", True).otherwise(False).alias("is_active"),
        )
        .dropDuplicates(["product_id"])
    )


def clean_stores(df):
    return (
        df.select(
            col("store_id"),
            trim(col("store_name")).alias("store_name"),
            trim(col("city")).alias("city"),
            trim(col("state")).alias("state"),
            trim(col("region")).alias("region"),
            col("opening_date"),
            trim(col("store_type")).alias("store_type"),
        )
        .dropDuplicates(["store_id"])
    )


def build_date_dimension(spark: SparkSession):
    orders = spark.table("raw_orders").select(col("order_date").alias("date"))
    events = spark.table("raw_clickstream").select(to_date(col("event_timestamp")).alias("date"))
    payments = spark.table("raw_payments").select(to_date(col("payment_timestamp")).alias("date"))
    dates = orders.union(events).union(payments).distinct().filter(col("date").isNotNull())

    return (
        dates.select(
            col("date"),
            year(col("date")).alias("year"),
            quarter(col("date")).alias("quarter"),
            month(col("date")).alias("month"),
            dayofmonth(col("date")).alias("day"),
            dayofweek(col("date")).alias("day_of_week"),
            (dayofweek(col("date")).isin(1, 7)).alias("is_weekend"),
        )
        .dropDuplicates(["date"])
    )


def build_fact_orders(spark: SparkSession):
    orders = spark.table("raw_orders")
    payments = spark.table("raw_payments").select(
        col("order_id"),
        col("payment_id").alias("payment_reference"),
        col("payment_method"),
        col("payment_status"),
        col("failure_reason"),
        col("payment_timestamp"),
        col("amount_paid"),
    )

    fact_orders = (
        orders.join(payments, on="order_id", how="left")
        .select(
            orders.order_id,
            orders.customer_id,
            orders.store_id,
            orders.order_channel,
            orders.order_source,
            orders.order_date,
            orders.order_timestamp,
            orders.order_status,
            orders.total_amount,
            orders.payment_id,
            payments.payment_method,
            payments.payment_status,
            payments.failure_reason,
            payments.payment_timestamp,
            payments.amount_paid,
        )
        .withColumn("order_year", year(col("order_date")))
        .withColumn("order_month", month(col("order_date")))
    )

    return fact_orders


def build_fact_order_items(spark: SparkSession):
    order_items = spark.table("raw_order_items")
    orders = spark.table("raw_orders").select(
        "order_id",
        "customer_id",
        "store_id",
        "order_date",
        "order_status",
        "order_channel",
    )
    products = spark.table("raw_products").select(
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "brand",
    )

    return (
        order_items.join(orders, on="order_id", how="left")
        .join(products, on="product_id", how="left")
        .select(
            order_items.order_item_id,
            order_items.order_id,
            order_items.product_id,
            col("product_name"),
            col("category"),
            col("sub_category"),
            col("brand"),
            order_items.quantity,
            order_items.item_price.alias("unit_price"),
            order_items.discount_amount,
            order_items.line_total,
            orders.customer_id,
            orders.store_id,
            orders.order_date,
            orders.order_status,
            orders.order_channel,
        )
        .withColumn(
            "discount_rate",
            when(
                (col("quantity") * col("unit_price")) == 0,
                None,
            ).otherwise(col("discount_amount") / (col("quantity") * col("unit_price"))),
        )
    )


def build_fact_clickstream(spark: SparkSession):
    clickstream = spark.table("raw_clickstream")
    return (
        clickstream.select(
            col("event_id"),
            col("customer_id"),
            col("session_id"),
            col("event_type"),
            col("product_id"),
            col("event_timestamp"),
            to_date(col("event_timestamp")).alias("event_date"),
            lower(trim(col("device_type"))).alias("device_type"),
            trim(col("location_city")).alias("location_city"),
            trim(col("page_url")).alias("page_url"),
            trim(col("referrer_source")).alias("referrer_source"),
        )
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    print("Spark session created with warehouse:", WAREHOUSE_DIR)

    validate_raw_tables(spark)

    dim_customers = clean_customers(spark.table("raw_customers"))
    write_table(dim_customers, "dim_customers")

    dim_products = clean_products(spark.table("raw_products"))
    write_table(dim_products, "dim_products")

    dim_stores = clean_stores(spark.table("raw_stores"))
    write_table(dim_stores, "dim_stores")

    dim_date = build_date_dimension(spark)
    write_table(dim_date, "dim_date")

    fact_orders = build_fact_orders(spark)
    write_table(fact_orders, "fact_orders")

    fact_order_items = build_fact_order_items(spark)
    write_table(fact_order_items, "fact_order_items")

    fact_clickstream = build_fact_clickstream(spark)
    write_table(fact_clickstream, "fact_clickstream")

    print("Cleaned/enriched tables created:")
    spark.sql("SHOW TABLES").show(truncate=False)

    print("Sample rows from fact_orders:")
    spark.table("fact_orders").show(5, truncate=False)

    spark.stop()
    print("Transform complete.")


if __name__ == "__main__":
    main()
