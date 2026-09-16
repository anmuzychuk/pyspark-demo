from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.master("local[4]")
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.2.0")
    .config("spark.sql.files.maxPartitionBytes", "100000")
    .getOrCreate()
)
df = spark.read.format("avro").load("data/users_big.avro")
print(df.rdd.getNumPartitions())  # 3
(
    df.groupBy(F.spark_partition_id().alias("partition"))
    .agg(F.count("*").alias("rows"), F.min("id").alias("min_id"), F.max("id").alias("max_id"))
    .orderBy("partition")
    .show()
)


print("Writing to Parquet...")
df.write.format("parquet").mode("overwrite").save("data/users_big.parquet")
print("done")
