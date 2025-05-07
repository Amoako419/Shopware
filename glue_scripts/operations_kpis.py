import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, from_unixtime, sum, avg, lit
from datetime import datetime
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.context import SparkContext
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

# Configure logging to CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a Spark session with S3 support
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
DELTA_OUTPUT_PATH = f"s3a://shopware-gold-layer-125/operation-kpis/"
POS_DATABASE = "shopware-silver-db"
POS_TABLE = "silver_pos"
INV_TABLE = "silver_inventory"


# Reading from AWS Glue Catalog
try:
    dyf_pos = glueContext.create_dynamic_frame.from_catalog(
        database=POS_DATABASE,
        table_name=POS_TABLE,
        additional_options={"version"="2"}
    )

    dyf_inv = glueContext.create_dynamic_frame.from_catalog(
        database=POS_DATABASE,
        table_name=INV_TABLE,
        additional_options={"version"="4"}
    )
    logger.info("Data read from catalog successfully.")

    # Converting to DataFrames
    df_pos = dyf_pos.toDF()
    df_inv = dyf_inv.toDF()
    logger.info("Data converted to DataFrames successfully.")

    # Casting and Cleaning
    df_pos = df_pos.select(
        col("transaction_id"),
        col("store_id").cast("int"),
        col("product_id").cast("int"),
        col("quantity").cast("int"),
        col("revenue").cast("double"),
        col("discount_applied").cast("double"),
        from_unixtime(col("timestamp")).alias("transaction_time")
    )

    df_inv = df_inv.select(
        col("inventory_id").cast("int"),
        col("product_id").cast("int"),
        col("warehouse_id").cast("int"),
        col("stock_level").cast("int"),
        col("restock_threshold").cast("int"),
        from_unixtime(col("last_updated")).alias("last_updated_time")
    )
    logger.info("Data cast and cleaned successfully.")


    # KPI Calculations

    # Inventory Turnover
    sales_per_product = df_pos.groupBy("product_id").agg(
        count("*").alias("transactions"),
        sum("quantity").alias("units_sold")
    )

    stock_per_product = df_inv.groupBy("product_id").agg(
        avg("stock_level").alias("avg_stock_level")
    )

    inventory_turnover = sales_per_product.join(
        stock_per_product, on="product_id", how="inner"
    ).withColumn(
        "inventory_turnover", col("units_sold") / col("avg_stock_level")
    )

    # Restock Frequency
    restock_events = df_inv.withColumn(
        "restock_event",
        when(col("restock_threshold").isNull(), lit(0))
        .when(col("stock_level") <= col("restock_threshold"), lit(1))
        .otherwise(lit(0))
    ).groupBy("product_id").agg(
        sum("restock_event").alias("restock_frequency")
    )

    # Stockout Alerts
    stockouts = df_inv.filter(col("stock_level") == 0).select("product_id").distinct()

    sold_out_products = df_pos.select("product_id").distinct().join(stockouts, "product_id", "inner")

    stockout_alerts = sold_out_products.withColumn("stockout_alert", lit(1))

    # Combine All Metrics
    final_metrics = inventory_turnover.join(
        restock_events, on="product_id", how="left"
    ).join(
        stockout_alerts, on="product_id", how="left"
    ).na.fill(0)

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

    # Construct dynamic Delta path
    delta_partitioned_path = f"{DELTA_OUTPUT_PATH}{timestamp}/"

    # Write to Delta Lake (time-partitioned path)
    final_metrics.write.format("delta") \
        .mode("overwrite") \
        .save(delta_partitioned_path)

    job.commit()

except Exception as e:
    logger.error(f"Glue job failed: {str(e)}")