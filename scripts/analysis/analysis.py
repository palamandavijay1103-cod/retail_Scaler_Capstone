from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    desc,
    sum as spark_sum,
    when,
    year,
    month,
)

OUTPUT_DIR = Path("business_output") / "analysis"
WAREHOUSE_DIR = Path("spark_warehouse").resolve()
SCRATCH_DIR = Path("/tmp/spark_scratch").resolve()

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
        .appName("retail_capstone_analysis")
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
            "Missing cleaned/enriched tables. Run transforms before analysis: "
            + ", ".join(missing)
        )


def table_counts(spark: SparkSession):
    counts = []
    for table in TABLES:
        counts.append((table, spark.table(table).count()))
    return counts


def order_status_summary(spark: SparkSession):
    return (
        spark.table("fact_orders")
        .groupBy("order_status")
        .agg(
            count("order_id").alias("order_count"),
            spark_sum("total_amount").alias("total_order_amount"),
            spark_sum("amount_paid").alias("total_amount_paid"),
        )
        .orderBy(desc("order_count"))
    )


def revenue_by_channel(spark: SparkSession):
    return (
        spark.table("fact_orders")
        .groupBy("order_channel")
        .agg(
            count("order_id").alias("orders"),
            spark_sum("total_amount").alias("total_amount"),
            spark_sum("amount_paid").alias("amount_paid"),
        )
        .orderBy(desc("total_amount"))
    )


def top_products_by_revenue(spark: SparkSession, top_n=10):
    return (
        spark.table("fact_order_items")
        .groupBy("product_id")
        .agg(
            count("order_item_id").alias("order_item_count"),
            spark_sum("line_total").alias("revenue"),
        )
        .orderBy(desc("revenue"))
        .limit(top_n)
    )


def customer_summary(spark: SparkSession):
    return (
        spark.table("dim_customers")
        .groupBy("customer_type", "is_active")
        .agg(count("customer_id").alias("customer_count"))
        .orderBy(desc("customer_count"))
    )


def monthly_order_trend(spark: SparkSession):
    return (
        spark.table("fact_orders")
        .withColumn("year", year(col("order_date")))
        .withColumn("month", month(col("order_date")))
        .groupBy("year", "month")
        .agg(
            count("order_id").alias("orders"),
            spark_sum("total_amount").alias("revenue"),
        )
        .orderBy("year", "month")
    )


def payment_status_summary(spark: SparkSession):
    return (
        spark.table("fact_orders")
        .groupBy("payment_status")
        .agg(
            count("order_id").alias("orders"),
            spark_sum("amount_paid").alias("amount_paid"),
        )
        .orderBy(desc("orders"))
    )


def write_summary(summary: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = OUTPUT_DIR / "analysis_summary.md"
    summary_path.write_text(summary)
    print(f"Wrote analysis summary to {summary_path}")


def df_to_markdown(df):
    columns = list(df.columns)
    header = "| " + " | ".join(columns) + " |\n"
    separator = "| " + " | ".join(["---" for _ in columns]) + " |\n"
    rows = [
        "| " + " | ".join(
            ["" if v is None else str(v) for v in row]
        ) + " |\n"
        for row in df.itertuples(index=False, name=None)
    ]
    return [header, separator] + rows


def main():
    spark = create_spark_session()
    validate_tables(spark)

    counts = table_counts(spark)
    status = order_status_summary(spark).toPandas()
    channel = revenue_by_channel(spark).toPandas()
    top_products = top_products_by_revenue(spark).toPandas()
    customers = customer_summary(spark).toPandas()
    trend = monthly_order_trend(spark).toPandas()
    payments = payment_status_summary(spark).toPandas()

    summary_lines = [
        "# Retail Capstone Analysis Summary\n",
        "## Table Counts\n",
        "| Table | Rows |\n",
        "|---|---:|\n",
    ]
    summary_lines += [f"| {table} | {rows} |\n" for table, rows in counts]
    summary_lines += ["\n## Order Status Summary\n"]
    summary_lines += df_to_markdown(status)
    summary_lines += ["\n## Revenue by Order Channel\n"]
    summary_lines += df_to_markdown(channel)
    summary_lines += ["\n## Top Products by Revenue\n"]
    summary_lines += df_to_markdown(top_products)
    summary_lines += ["\n## Customer Segment Summary\n"]
    summary_lines += df_to_markdown(customers)
    summary_lines += ["\n## Monthly Order Trend\n"]
    summary_lines += df_to_markdown(trend)
    summary_lines += ["\n## Payment Status Summary\n"]
    summary_lines += df_to_markdown(payments)

    summary_text = "".join(summary_lines)
    write_summary(summary_text)

    spark.stop()


if __name__ == "__main__":
    main()
