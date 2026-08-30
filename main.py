from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("pyspark-demo").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data = [("Alice", 34), ("Bob", 28), ("Carol", 41)]
    df = spark.createDataFrame(data, ["name", "age"])

    df.show()
    print("Average age:", df.groupBy().avg("age").collect()[0][0])

    spark.stop()


if __name__ == "__main__":
    main()
