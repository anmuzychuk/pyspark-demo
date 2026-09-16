from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from pyspark_demo.display_utils import display_dataframe

builder = (
    SparkSession.builder.master("local[4]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.log.level", "ERROR")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()


source_path = Path("data/users_big.delta").resolve()

# Create a DeltaTable object for the specified path and retrieve its history
delta_table = DeltaTable.forPath(spark, str(source_path))

n_rows = delta_table.toDF().count()
print(f"Total number of rows in the Delta table: {n_rows}")

# remove first 100 users from the Delta table
delta_table.delete("id < 100")
n_rows_after_delete = delta_table.toDF().count()
print(f"Total number of rows in the Delta table: {n_rows_after_delete}")


# print the history of the Delta table
delta_history_df = delta_table.history()
display_dataframe(delta_history_df.toPandas())

# Display the history of the Delta table
display_dataframe(delta_history_df.toPandas())
delta_history_df.write.format("json").mode("overwrite").save("data/delta_history.json")


# # time traveling to a specific version of the Delta table
# version_to_travel = 0  # Specify the version you want to travel to
# # Read the Delta table at the specified version
# delta_table_versioned_df = spark.read.format("delta").option("versionAsOf", version_to_travel).load(str(source_path))
