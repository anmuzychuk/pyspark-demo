# Read data/users_big.avro and write it as a Delta table to data/users_big.delta
#
# Delta Lake ships as a Spark package, like spark-avro. The artifact name carries the Spark version:
# delta-spark_4.2_2.13:4.4.0 = Delta 4.4.0 built for Spark 4.2 and Scala 2.13.
#
# Run from the repo root:
#   uv run python labs/L3/delta_example.py

from pathlib import Path

from pyspark.sql import SparkSession

from pyspark_demo.display_utils import display_dataframe

# Absolute path: SQL's delta.`<path>` syntax does not accept relative paths
table_path = Path("data/users_big.delta").resolve()

spark = (
    SparkSession.builder.master("local[4]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-avro_2.13:4.2.0,io.delta:delta-spark_4.2_2.13:4.4.0",
    )
    # Enable Delta SQL commands (DESCRIBE HISTORY, VACUUM, ...) and the Delta catalog
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Same small split size as spark_avro_read_parquet_write.py: 3 partitions -> 3 data files
    .config("spark.sql.files.maxPartitionBytes", "100000")
    .getOrCreate()
)

df = spark.read.format("avro").load("data/users_big.avro")
print("partitions:", df.rdd.getNumPartitions())

print("Writing to Delta...")
df.write.format("delta").mode("overwrite").save(str(table_path))

print("Reading back...")
spark.read.format("delta").load(str(table_path)).show(5)


hist = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`").select(
    "version", "operation", "operationParameters", "operationMetrics"
)

display_dataframe(hist.toPandas())

spark.stop()
