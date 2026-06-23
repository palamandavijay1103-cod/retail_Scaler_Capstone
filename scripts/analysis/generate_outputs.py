from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    desc,
    sum as spark_sum,
    when,
    year,
    month,
)

WAREHOUSE_DIR = Path("spark_warehouse").resolve()
SCRATCH_DIR = Path("/tmp/spark_scratch").resolve()
OUTPUT_ROOT = Path("business_output")

TABLES = [
    "dim_customers",
    "dim_products",
    "dim_stores",
    "dim_date",
    "fact_orders",
    "fact_order_items",
    "fact_clickstream",
]


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("retail_capstone_outputs")
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


def validate_tables(spark: SparkSession):
    missing = [t for t in TABLES if not spark.catalog.tableExists(t)]
    if missing:
        raise RuntimeError(
            "Missing cleaned/enriched tables. Run the transform pipeline before generating outputs: "
            + ", ".join(missing)
        )


def write_output(df, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "data.parquet"
    df.write.mode("overwrite").parquet(str(path))
    print(f"Wrote {output_dir} to {path}")


def top_products_by_revenue(spark: SparkSession):
    return (
        spark.table("fact_order_items")
        .groupBy("product_id")
        .agg(
            countDistinct("order_id").alias("orders"),
            spark_sum("line_total").alias("revenue"),
        )
        .join(spark.table("dim_products"), on="product_id", how="left")
        .select(
            "product_id",
            "product_name",
            "category",
            "sub_category",
            "brand",
            "orders",
            "revenue",
        )
        .orderBy(desc("revenue"))
    )


def top_customers_by_spending(spark: SparkSession):
    return (
        spark.table("fact_orders")
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("orders"),
            spark_sum("total_amount").alias("total_spent"),
            spark_sum("amount_paid").alias("amount_paid"),
        )
        .join(spark.table("dim_customers"), on="customer_id", how="left")
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "customer_type",
            "is_active",
            "orders",
            "total_spent",
            "amount_paid",
        )
        .orderBy(desc("total_spent"))
    )


def top_stores(spark: SparkSession):
    return (
        spark.table("fact_orders")
        .filter(col("store_id").isNotNull())
        .groupBy("store_id")
        .agg(
            countDistinct("order_id").alias("orders"),
            spark_sum("total_amount").alias("revenue"),
        )
        .join(spark.table("dim_stores"), on="store_id", how="left")
        .select(
            "store_id",
            "store_name",
            "city",
            "state",
            "region",
            "store_type",
            "orders",
            "revenue",
        )
        .orderBy(desc("revenue"))
    )


def repeat_customer_percent(spark: SparkSession):
    orders = (
        spark.table("fact_orders")
        .groupBy("customer_id")
        .agg(countDistinct("order_id").alias("order_count"))
    )
    total_customers = orders.filter(col("order_count") >= 1).count()
    repeat_customers = orders.filter(col("order_count") > 1).count()
    percent = float(repeat_customers) / float(total_customers) if total_customers else 0.0
    return spark.createDataFrame(
        [(total_customers, repeat_customers, percent)],
        ["total_customers", "repeat_customers", "repeat_customer_percent"],
    )


def clv(spark: SparkSession):
    return (
        spark.table("fact_orders")
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("orders"),
            spark_sum("total_amount").alias("total_spent"),
            spark_sum("amount_paid").alias("amount_paid"),
        )
        .withColumn("clv", col("total_spent"))
        .join(spark.table("dim_customers"), on="customer_id", how="left")
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "orders",
            "total_spent",
            "amount_paid",
            "clv",
        )
        .orderBy(desc("clv"))
    )


def aov(spark: SparkSession):
    orders = spark.table("fact_orders")
    return (
        orders
        .agg(
            countDistinct("order_id").alias("distinct_orders"),
            spark_sum("total_amount").alias("total_revenue"),
        )
        .withColumn("average_order_value", col("total_revenue") / col("distinct_orders"))
    )


def monthly_active_customers(spark: SparkSession):
    return (
        spark.table("fact_orders")
        .withColumn("order_year", year(col("order_date")))
        .withColumn("order_month", month(col("order_date")))
        .groupBy("order_year", "order_month")
        .agg(countDistinct("customer_id").alias("active_customers"))
        .orderBy("order_year", "order_month")
    )


def revenue_per_store(spark: SparkSession):
    return (
        spark.table("fact_orders")
        .filter(col("store_id").isNotNull())
        .groupBy("store_id")
        .agg(
            countDistinct("order_id").alias("orders"),
            spark_sum("total_amount").alias("revenue"),
        )
        .join(spark.table("dim_stores"), on="store_id", how="left")
        .select(
            "store_id",
            "store_name",
            "city",
            "state",
            "region",
            "store_type",
            "orders",
            "revenue",
        )
        .orderBy(desc("revenue"))
    )


def revenue_per_region(spark: SparkSession):
    return (
        spark.table("fact_orders")
        .filter(col("store_id").isNotNull())
        .join(spark.table("dim_stores"), on="store_id", how="left")
        .groupBy("region")
        .agg(
            countDistinct("order_id").alias("orders"),
            spark_sum("total_amount").alias("revenue"),
        )
        .orderBy(desc("revenue"))
    )


def main():
    output_root = OUTPUT_ROOT / "analytics_output"
    spark = create_spark_session()
    validate_tables(spark)

    outputs = [
        (top_products_by_revenue(spark), output_root / "top_products_by_revenue"),
        (top_customers_by_spending(spark), output_root / "top_customers_by_spending"),
        (top_stores(spark), output_root / "top_stores"),
        (repeat_customer_percent(spark), output_root / "repeat_customer_percent"),
        (clv(spark), output_root / "clv"),
        (aov(spark), output_root / "aov"),
        (monthly_active_customers(spark), output_root / "monthly_active_customers"),
        (revenue_per_store(spark), output_root / "revenue_per_store"),
        (revenue_per_region(spark), output_root / "revenue_per_region"),
    ]

    for df, output_dir in outputs:
        write_output(df, output_dir)

    spark.stop()


if __name__ == "__main__":
    main()
