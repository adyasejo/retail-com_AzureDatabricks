# Complete ADLS Batch Ingestion Example
from pyspark.sql.functions import col, current_timestamp, year, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark.sql("USE CATALOG analytics_dev")

storage_account = "joeladlsdbriksdeveastus"
container = "rawdata"

# Authentication is handled by the Unity Catalog External Location for this storage account.

# Define paths
base_path = f"abfss://external@joeladlsdbriksdeveastus.dfs.core.windows.net/analytics_dev/raw/rawdata"
sales_csv_path = f"{base_path}/sales/csv/"
products_json_path = f"{base_path}/products/json/"
delta_output_path = f"{base_path}/delta/sales_enriched/"

# Define schema for CSV
sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("sale_date", TimestampType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False)
])

# Read CSV sales data
print("Reading CSV sales data...")
sales_df = spark.read \
    .option("header", "true") \
    .schema(sales_schema) \
    .csv(sales_csv_path)

# Read JSON product data
print("Reading JSON product data...")
products_df = spark.read \
    .option("multiLine", "true") \
    .json(products_json_path)

# Enrich and transform
sales_enriched = sales_df \
    .withColumn("total_amount", col("quantity") * col("unit_price")) \
    .withColumn("sale_year", year("sale_date")) \
    .withColumn("sale_month", month("sale_date")) \
    .withColumn("ingestion_timestamp", current_timestamp())

# Join with product information
sales_with_products = sales_enriched \
    .join(products_df, sales_enriched["product_id"] == products_df["id"], "left")

# Write to Delta Lake with partitioning
print("Writing to Delta Lake...")
sales_with_products.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("sale_year", "sale_month") \
    .option("overwriteSchema", "true") \
    .saveAsTable("sales_db.sales_enriched")
print("Data loaded successfully!")
display(spark.sql("SELECT * FROM sales_db.sales_enriched LIMIT 10"))
