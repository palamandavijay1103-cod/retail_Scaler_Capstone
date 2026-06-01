from pyspark.sql import SparkSession
from pathlib import Path

RAW_DIR = Path('raw_data')
OUTPUT_DIR = Path('hdfs_output')
TEST_PATH = OUTPUT_DIR / 'spark_test'

RAW_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

spark = SparkSession.builder \
    .appName('spark_test') \
    .master('local[1]') \
    .config('spark.sql.warehouse.dir', str(Path('spark_warehouse').resolve())) \
    .enableHiveSupport() \
    .getOrCreate()

print('spark version', spark.version)

rows = [(1, 'test', 100.0), (2, 'sample', 200.5)]
columns = ['id', 'name', 'amount']

df = spark.createDataFrame(rows, columns)
df.write.mode('overwrite').parquet(str(TEST_PATH))

print('Written test parquet to', TEST_PATH)

read_df = spark.read.parquet(str(TEST_PATH))
read_df.show()

spark.stop()
