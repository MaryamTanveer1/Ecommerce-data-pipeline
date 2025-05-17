from awsglue.context import GlueContext
from pyspark.context import SparkContext

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

input_path = "s3://ecom-data-pipeline-bucket/raw/json/"
output_path = "s3://ecom-data-pipeline-bucket/processed/parquet/"

df = spark.read.option("multiline", "true").json(input_path)

df.printSchema()
df.show(5)

df.write.mode("overwrite").parquet(output_path)

from pyspark.sql.functions import col, to_timestamp

base_path = "s3://ecom-data-pipeline-bucket/raw/json/"

customers = spark.read.json(base_path + "olist_customers_dataset.json")
geolocation = spark.read.json(base_path + "olist_geolocation_dataset.json")
order_items = spark.read.json(base_path + "olist_order_items_dataset.json")
payments = spark.read.json(base_path + "olist_order_payments_dataset.json")
reviews = spark.read.json(base_path + "olist_order_reviews_dataset.json")
orders = spark.read.json(base_path + "olist_orders_dataset.json")
products = spark.read.json(base_path + "olist_products_dataset.json")
sellers = spark.read.json(base_path + "olist_sellers_dataset.json")
category_translation = spark.read.json(base_path + "product_category_name_translation.json")

df = orders \
    .join(customers, "customer_id", "left") \
    .join(order_items, "order_id", "left") \
    .join(products, "product_id", "left") \
    .join(sellers, "seller_id", "left") \
    .join(payments, "order_id", "left") \
    .join(reviews, "order_id", "left") \
    .join(category_translation, "product_category_name", "left") \
    .join(geolocation, customers["customer_zip_code_prefix"] == geolocation["geolocation_zip_code_prefix"], "left")

df_cleaned = df.filter(col("order_id").isNotNull())

date_cols = [
    "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date", "shipping_limit_date",
    "review_creation_date", "review_answer_timestamp"
]

for c in date_cols:
    if c in df_cleaned.columns:
        df_cleaned = df_cleaned.withColumn(c, to_timestamp(col(c)))

output_path = "s3://ecom-data-pipeline-bucket/processed/parquet/"
df_cleaned.write.mode("overwrite").parquet(output_path)

df_cleaned.select("order_id", "customer_id", "order_status", "order_purchase_timestamp").show(5)