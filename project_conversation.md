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
- Spark test previously failed due to Hadoop UGI/Hive warehouse path issues

## Next steps
1. Run `python3 spark_test.py` to confirm the fix
2. If the test succeeds, design Hive table schemas and ingest raw CSV data
3. Implement ETL and cleaning pipelines in PySpark
4. Perform exploratory analysis and build the Capstone deliverables

## Notes
- Prefer updating this file with any new decisions or changed assumptions
- `spark_test.py` was updated to use `spark.hadoop.fs.defaultFS=file:///` and an absolute warehouse path
- The failure was ultimately caused by the runtime Java version: Spark/Hive needs Java 17 in this workspace, while the shell default Java was Java 25
- The fix is to run Spark under `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` (or via `source setup_env.sh`)
- This file is the shared project memory for future reference. Keep it updated with setup decisions, environment fixes, and next steps so the process does not need to be repeated.
