import sys
import logging
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, sum, avg, count, when, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, DoubleType
from delta.tables import DeltaTable

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = logging.getLogger('SalesKPIsGlueJob')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def define_schemas():
    """Define schemas for POS and Inventory data."""
    pos_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("store_id", IntegerType(), False),
        StructField("product_id", LongType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("revenue", FloatType(), False),
        StructField("discount_applied", FloatType(), True),
        StructField("timestamp", DoubleType(), False)
    ])
    
    inventory_schema = StructType([
        StructField("inventory_id", IntegerType(), False),
        StructField("product_id", LongType(), False),
        StructField("warehouse_id", IntegerType(), False),
        StructField("stock_level", IntegerType(), False),
        StructField("restock_threshold", IntegerType(), True),
        StructField("last_updated", DoubleType(), False)
    ])
    logger.info("Schemas for POS and Inventory data defined.")
    return pos_schema, inventory_schema
    

def read_data(spark, pos_path, inventory_path, pos_schema, inventory_schema):
    """Read POS and Inventory data from S3."""
    try:
        pos_df = (spark.read
                  .schema(pos_schema)
                  .parquet(pos_path)
                  .filter(to_date(from_unixtime(col("timestamp") / 1000)).isNotNull()))
        logger.info(f"Successfully read POS data from {pos_path} with {pos_df.count()} records.")
        
        inventory_df = (spark.read
                        .schema(inventory_schema)
                        .parquet(inventory_path)
                        .withColumn("last_updated", col("last_updated").cast(LongType()))
                        .filter(to_date(from_unixtime(col("last_updated") / 1000)).isNotNull()))
        logger.info(f"Successfully read Inventory data from {inventory_path} with {inventory_df.count()} records.")
        
        return pos_df, inventory_df
    except Exception as e:
        logger.error(f"Failed to read data from S3: {str(e)}")
        raise

def compute_total_sales(pos_df):
    """Compute Total Sales by Region (store_id) and Product."""
    try:
        total_sales_df = (pos_df.groupBy(
                            to_date(from_unixtime(col("timestamp"))).alias("date"),
                            col("store_id"),
                            col("product_id"))
                         .agg(sum("revenue").alias("total_sales"))
                         .withColumn("kpi_name", lit("total_sales")))
        logger.info("Computed Total Sales KPI.")
        return total_sales_df
    except Exception as e:
        logger.error(f"Failed to compute Total Sales KPI: {str(e)}")
        raise

def compute_stock_availability(inventory_df):
    """Compute Stock Availability (% of products above restock threshold)."""
    try:
        stock_availability_df = (inventory_df.groupBy(
                                    to_date(from_unixtime(col("last_updated"))).alias("date"),
                                    col("product_id"))
                                .agg(
                                    avg(when(col("stock_level") > col("restock_threshold"), 1.0)
                                        .otherwise(0.0) * 100).alias("stock_availability_pct")
                                )
                                .withColumn("kpi_name", lit("stock_availability")))
        logger.info("Computed Stock Availability KPI.")
        return stock_availability_df
    except Exception as e:
        logger.error(f"Failed to compute Stock Availability KPI: {str(e)}")
        raise

def compute_product_turnover(pos_df, inventory_df):
    """Compute Product Turnover Rate (units sold / avg stock level)."""
    try:
        # Aggregate units sold from POS
        units_sold_df = (pos_df.groupBy(
                            to_date(from_unixtime(col("timestamp"))).alias("date"),
                            col("product_id"))
                        .agg(sum("quantity").alias("units_sold")))
        
        # Aggregate average stock level from Inventory
        avg_stock_df = (inventory_df.groupBy(
                            to_date(from_unixtime(col("last_updated"))).alias("date"),
                            col("product_id"))
                       .agg(avg("stock_level").alias("avg_stock_level")))
        
        # Join and compute turnover rate
        turnover_df = (units_sold_df.join(avg_stock_df, ["date", "product_id"], "inner")
                      .withColumn("turnover_rate", col("units_sold") / col("avg_stock_level"))
                      .select("date", "product_id", "turnover_rate")
                      .withColumn("kpi_name", lit("product_turnover")))
        logger.info("Computed Product Turnover Rate KPI.")
        return turnover_df
    except Exception as e:
        logger.error(f"Failed to compute Product Turnover Rate KPI: {str(e)}")
        raise

def save_to_delta(spark, df, output_path, partition_cols=["date"]):
    """Save DataFrame as Delta table in S3."""
    try:
        df.write.option("mergeSchema", "true") \
           .format("delta") \
           .partitionBy(partition_cols) \
           .mode("overwrite") \
           .save(output_path)
        logger.info(f"Successfully saved Delta table to {output_path}.")
        
        # Optimize Delta table
        delta_table = DeltaTable.forPath(spark, output_path)
        delta_table.vacuum(168)  # Retain 7 days of history
        logger.info("Delta table optimized and vacuumed.")
    except Exception as e:
        logger.error(f"Failed to save Delta table to {output_path}: {str(e)}")
        raise

def main():
    """Main function to orchestrate KPI computation and storage."""
    # Configuration
    pos_path = "s3://shopware-silver-layer-125/silver-pos/*"
    inventory_path = "s3://shopware-silver-layer-125/silver-inventory/*"
    output_path = "s3://shopware-gold-layer-125/sales_kpis/"
    
    try:
        
        # Define schemas
        pos_schema, inventory_schema = define_schemas()
        
        # Read data
        pos_df, inventory_df = read_data(spark, pos_path, inventory_path, pos_schema, inventory_schema)
        
        # Compute KPIs
        total_sales_df = compute_total_sales(pos_df)
        stock_availability_df = compute_stock_availability(inventory_df)
        turnover_df = compute_product_turnover(pos_df, inventory_df)
        
        # Union all KPIs
        kpi_df = (total_sales_df.unionByName(stock_availability_df, allowMissingColumns=True)
                                .unionByName(turnover_df, allowMissingColumns=True))
        logger.info("All KPIs combined into a single DataFrame.")
        
        # Save to Delta table
        save_to_delta(spark, kpi_df, output_path)
        
        logger.info("Sales KPIs Glue job completed successfully.")
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")
        job.commit()

if __name__ == "__main__":
    main()
