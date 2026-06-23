# retail_Scaler_Capstone
Capstone project from Scaler Data Engineering Module

## Raw Data Source
The raw input datasets are not stored in this repository due to file size constraints.

Download the raw files into `raw_data/` from the Google Drive folder:

https://drive.google.com/drive/u/0/folders/1oPBELdKx2gcZ6Uamur45729EXIt7j716

## Running Spark tests
Before using PySpark/Hive, load the project Java environment:

```bash
source ./scripts/setup/setup_env.sh
```

Then run:

```bash
bash ./scripts/setup/run_spark_test.sh
```

## Running ETL ingestion
```bash
bash ./scripts/setup/run_etl.sh
```

## Running cleaned/enriched transforms
```bash
bash ./scripts/setup/run_transform.sh
```

## Running analysis
```bash
bash ./scripts/setup/run_analysis.sh
```
