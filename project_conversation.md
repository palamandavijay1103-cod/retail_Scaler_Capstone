# Project Conversation Log

## Purpose
This file captures our Capstone project decisions, setup steps, and the current state of the work so we don't have to repeat the full process every time.

## Summary
- Project based on `Lecture 24 - Capstone.pdf`
- Environment setup for PySpark and Hive has been performed
- Raw CSV data files were uploaded to `raw_data/`
- Local git changes were committed and pushed to GitHub before proceeding

## Current status
- `spark_test.py` now uses a `file:///` absolute warehouse path for Spark
- `hdfs_output/` and `spark_warehouse/` are present locally
- `etl.py` was added to define Hive table schemas and ingest raw CSV data
- Raw data from `raw_data/` has been ingested into Hive tables and written as Parquet
- `scripts/transform/clean_enriched.py` was added and the cleaned/enriched transform completed successfully
- Spark test, ETL pipeline, and transform pipeline all ran successfully in the Java 17 environment

## Project checklist
- [x] Initialize project and upload raw CSV files to `raw_data/`
- [x] Set up PySpark/Hive environment and Java 17 configuration
- [x] Validate Spark configuration with `spark_test.py`
- [x] Create `etl.py` to define Hive schemas and ingest raw CSV data
- [x] Run ETL and create Hive tables plus Parquet outputs in `hdfs_output/ingested/`
- [x] Design cleaned and enriched Hive tables or analytics datasets
- [x] Implement PySpark transformations for business reporting
- [x] Perform exploratory data analysis on the ingested data
- [x] Build Capstone deliverables and document the pipeline

## Deliverables and required insights
- Required business insights:
  - top products by revenue
  - top customers by spending
  - top stores
  - repeat customer percentage
  - customer lifetime value (CLV)
  - average order value (AOV)
  - monthly active customers
  - revenue per store
  - revenue per region
  - optional store-wise conversion if clickstream alignment is implemented
- Required output organization:
  - `analytics_output/top_products_by_revenue/`
  - `analytics_output/top_customers_by_spending/`
  - `analytics_output/top_stores/`
  - `analytics_output/repeat_customer_percent/`
- `analytics_output/clv/`
- `analytics_output/aov/`
- `analytics_output/monthly_active_customers/`
- `analytics_output/revenue_per_store/`
- `analytics_output/revenue_per_region/`
- Preferred output format: Parquet for analytics-scale storage and querying
- Architecture notes from Lecture 24:
  - Raw → Staging → Analytics layered approach
  - Preserve raw data as immutable source-aligned storage
  - Clean, validate, and standardize in staging
  - Use star schema for analytics with fact and dimension tables
  - Partition by date keys and use columnar formats

## Next steps
1. Build Capstone deliverables and write analytic outputs to structured folders
2. Prepare summary documentation and final submission materials
3. Keep the project conversation updated with each milestone

## Notes
- Prefer updating this file with any new decisions or changed assumptions
- `spark_test.py` was updated to use `spark.hadoop.fs.defaultFS=file:///` and an absolute warehouse path
- The failure was ultimately caused by the runtime Java version: Spark/Hive needs Java 17 in this workspace, while the shell default Java was Java 25
- The fix is to run Spark under `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` (or via `source setup_env.sh`)
- This file is the shared project memory for future reference. Keep it updated with setup decisions, environment fixes, and next steps so the process does not need to be repeated.

## Update log
- Added `etl.py` to define Hive schemas and ingest raw CSV data into Hive tables.
- Ran `spark_test.py` successfully after Java 17 environment setup.
- Ran `etl.py` successfully and created Hive tables plus Parquet outputs in `hdfs_output/ingested/`.
- Next milestone: create cleaned and enriched tables for reporting and analytics.
