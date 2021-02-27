# Find out how many orders, how many products and how many sellers are in the data.
# How many products have been sold at least once? Which is the product contained in more orders?

from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()

products_count = spark.read.parquet("datasets/products_parquet/").count()
sellers_count = spark.read.parquet("datasets/sellers_parquet/").count()
sales_count = spark.read.parquet("datasets/sales_parquet/").count()

print(f"Products: {products_count}")
print(f"Sellers: {sellers_count}")
print(f"Sales: {sales_count}")
