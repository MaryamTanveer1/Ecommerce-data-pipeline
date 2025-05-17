import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

jdbc_url = "******"
jdbc_props = {
    "user": "admin",
    "password": "*******"
}

df = spark.read.parquet("s3://ecom-data-pipeline-bucket/processed/parquet/")

df.select("customer_id", "customer_unique_id", "customer_city", "customer_state") \
    .dropDuplicates(["customer_id"]) \
    .limit(50) \
    .write.jdbc(url=jdbc_url, table="customers", mode="append", properties=jdbc_props)

df.select("product_id", "product_category_name", "product_name_lenght", "product_description_lenght") \
    .dropDuplicates(["product_id"]) \
    .limit(50) \
    .write.jdbc(url=jdbc_url, table="products", mode="append", properties=jdbc_props)

df.select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state") \
    .dropna(subset=["seller_id"]) \
    .dropDuplicates(["seller_id"]) \
    .limit(50) \
    .write.jdbc(url=jdbc_url, table="sellers", mode="append", properties=jdbc_props)

df.select("order_id", "customer_id", "order_status", "order_purchase_timestamp") \
    .dropDuplicates(["order_id"]) \
    .limit(50) \
    .write.jdbc(url=jdbc_url, table="orders", mode="append", properties=jdbc_props)

df.select("order_id", "payment_type", "payment_value") \
    .dropna(subset=["order_id"]) \
    .limit(50) \
    .write.jdbc(url=jdbc_url, table="payments", mode="append", properties=jdbc_props)

df.select("review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date") \
    .dropna(subset=["review_id"]) \
    .limit(50) \
    .write.jdbc(url=jdbc_url, table="reviews", mode="append", properties=jdbc_props)

df.select("order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value") \
    .dropna(subset=["order_id", "order_item_id"]) \
    .dropDuplicates(["order_id", "order_item_id"]) \
    .limit(50) \
    .write.jdbc(url=jdbc_url, table="order_items", mode="append", properties=jdbc_props)

