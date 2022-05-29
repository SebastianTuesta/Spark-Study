from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("Customer").getOrCreate()

schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("amount_spent", FloatType(), True)
])

df = spark.read.schema(schema).csv("customer-orders.csv")

df.groupBy("cust_id").agg(
    func.round(func.sum("amount_spent"), 2).alias("total_spent")
).sort("total_spent").show()

spark.stop()

