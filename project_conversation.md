# Project Conversation Log

## Purpose
This file explains the repository, what work has been done, and how to continue the Capstone project.
It is intended for a new contributor who wants to understand the repo structure, data, and workflow.

## Repo overview
- The repo is based on `Lecture 24 - Capstone.pdf`.
- Raw input datasets are stored in `raw_data/`.
- The project is organized into script folders under `scripts/`.
- Generated outputs and Hive state are excluded from GitHub via `.gitignore`.

## Where the raw files are
- All raw CSV source files are in `raw_data/`.
- These raw files are the source data for ETL, transforms, and analytics.

## What we have done
- Set up the PySpark/Hive environment and fixed Java 17 compatibility.
- Validated Spark/Hive with `scripts/ingest/spark_test.py`.
- Added `scripts/ingest/etl.py` to read raw CSV files and create Hive tables.
- Added `scripts/transform/clean_enriched.py` to build cleaned/enriched analytics tables.
- Added `scripts/analysis/analysis.py` to summarize business metrics.
- Added `scripts/analysis/generate_outputs.py` to write final deliverables.
- Generated final analytics output files in `business_output/analytics_output/`.

## What is included in this repo
- `raw_data/` — raw source CSV files (source data layer)
- `scripts/setup/` — environment and runner scripts
- `scripts/ingest/` — raw ingestion and Hive table creation
- `scripts/transform/` — cleaned/enriched analytics table creation
- `scripts/analysis/` — business analysis and output generation
- `project_conversation.md` — project status and notes
- `README.md` — repository setup and run instructions

## Current status
- Raw data is available and ready in `raw_data/`.
- Raw ingestion is complete and stored as Hive tables.
- Cleaned/enriched analytics tables were built successfully.
- Analytics deliverables were generated successfully.
- The workspace was run under Java 17 and the Spark/Hive configuration is stable.

## Business insights
- Delivered orders are the largest revenue driver, with 300,263 delivered orders generating ~65.3B.
- The Online channel leads revenue with ~76.0B over 349,786 orders.
- The Store channel contributes ~32.7B over 150,214 orders.
- Top revenue products include `P-15695`, `P-14386`, `P-8757`, `P-18263`, and `P-6432`.
- Customer segments are evenly distributed across Offline, Online, and Both.
- Monthly order volume is stable over 2022–2024, with revenue near 2.9B–3.1B per month.
- Payment success is high, with 460,089 successful payments and 39,911 failures.

## Deliverable outputs
The final analytics deliverables are available under:
- `business_output/analytics_output/top_products_by_revenue/data.parquet`
- `business_output/analytics_output/top_customers_by_spending/data.parquet`
- `business_output/analytics_output/top_stores/data.parquet`
- `business_output/analytics_output/repeat_customer_percent/data.parquet`
- `business_output/analytics_output/clv/data.parquet`
- `business_output/analytics_output/aov/data.parquet`
- `business_output/analytics_output/monthly_active_customers/data.parquet`
- `business_output/analytics_output/revenue_per_store/data.parquet`
- `business_output/analytics_output/revenue_per_region/data.parquet`

## How to run the project
1. Set up the environment:
   ```bash
   source ./scripts/setup/setup_env.sh
   ```
2. Validate Spark/Hive:
   ```bash
   bash ./scripts/setup/run_spark_test.sh
   ```
3. Run raw ingestion:
   ```bash
   bash ./scripts/setup/run_etl.sh
   ```
4. Build cleaned/enriched tables:
   ```bash
   bash ./scripts/setup/run_transform.sh
   ```
5. Run analytics summary:
   ```bash
   bash ./scripts/setup/run_analysis.sh
   ```
6. Generate final deliverable outputs:
   ```bash
   bash ./scripts/setup/run_generate_outputs.sh
   ```

## Notes
- `business_output/`, `hdfs_output/`, `.vscode/`, `spark_warehouse/`, `metastore_db/`, and `derby.log` are ignored and not committed.
- This repo uses a layered approach: raw data, staging/cleaned data, and analytics outputs.
- Keep this file updated when new decisions or work are added.

## Update log
- Added script folders and organized the project by purpose.
- Added raw ingestion, transform, analysis, and output generation workflows.
- Generated analytics deliverables in `business_output/analytics_output/`.
- Documented source data location and current repository status clearly.
