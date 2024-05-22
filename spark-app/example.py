from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExampleApp") \
    .getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

spark.stop()
