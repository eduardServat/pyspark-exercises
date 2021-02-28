# What is the average revenue of the orders?

from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import avg, broadcast, when, lit, concat, rand, round
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()

products = spark.read.parquet("datasets/products_parquet/")
sales = spark.read.parquet("datasets/sales_parquet/")

REPLICATION_FACTOR = 101

skewed_keys = sales.groupby("product_id").count().orderBy("count").limit(100).collect()

replicated_products = [product["product_id"] for product in skewed_keys]
salted_keys = [(product_id, part_id) for product_id in replicated_products for part_id in range(REPLICATION_FACTOR)]

rdd = spark.sparkContext.parallelize(salted_keys)
replicated_df = rdd.map(lambda x: Row(product_id=x[0], replication=int(x[1])))
replicated_df = spark.createDataFrame(replicated_df)

products = products.join(
        other=broadcast(replicated_df),
        on=products["product_id"] == replicated_df["product_id"],
        how="left") \
    .withColumn(
        colName="salted_join_key",
        col=when(replicated_df["replication"].isNull(), products["product_id"])
            .otherwise(concat(replicated_df["product_id"], lit("-"), replicated_df["replication"]))
    )

sales = sales.withColumn(
    colName="salted_join_key",
    col=when(
        condition=sales["product_id"].isin(replicated_products),
        value=concat(sales["product_id"], lit("-"), round(rand() * (REPLICATION_FACTOR - 1), 0).cast(IntegerType()))
    ).otherwise(
        value=sales["product_id"]
    )
)

sales.join(
    other=products,
    on=sales["salted_join_key"] == products["salted_join_key"],
    how="inner"
).agg(avg(products["price"] * sales["num_pieces_sold"])).show()
