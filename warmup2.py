# How many distinct products have been sold in each day?

from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()

df = spark.read.parquet("datasets/sales_parquet/")

df = df.select("date", "product_id").distinct().groupby("date").count().sort("date")

df.show()
