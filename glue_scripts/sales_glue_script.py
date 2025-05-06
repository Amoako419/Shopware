import sys
import logging
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, sum, avg, count, when, to_date, lit, coalesce, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, DoubleType



args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'POS_PATH',
    'INVENTORY_PATH',
    'REDSHIFT_JDBC_URL',
    'REDSHIFT_USER',
    'REDSHIFT_PASSWORD'
])

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
        StructField("store_id", LongType(), False),
        StructField("product_id", LongType(), False),
        StructField("quantity", LongType(), False),
        StructField("revenue", DoubleType(), False),
        StructField("discount_applied", FloatType(), True),
        StructField("timestamp", DoubleType(), False)
    ])
    
    inventory_schema = StructType([
        StructField("inventory_id", IntegerType(), False),
        StructField("product_id", LongType(), False),
        StructField("warehouse_id", IntegerType(), False),
        StructField("stock_level", LongType(), False),
        StructField("restock_threshold", DoubleType(), True),
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
                  .filter(to_date(from_unixtime(col("timestamp"))).isNotNull()))
        logger.info(f"Successfully read POS data from {pos_path} with {pos_df.count()} records.")
        
        inventory_df = (spark.read
                        .schema(inventory_schema)
                        .parquet(inventory_path)
                        .filter(to_date(from_unixtime(col("last_updated"))).isNotNull()))
        logger.info(f"Successfully read Inventory data from {inventory_path} with {inventory_df.count()} records.")
        
        return pos_df, inventory_df
    except Exception as e:
        logger.error(f"Failed to read data from S3: {str(e)}")
        raise

def compute_total_sales(pos_df):
    """Compute Total Sales by Region (store_id) and Product."""
    try:
        total_sales_df = (
                         pos_df
                         .withColumn("date", to_date(from_unixtime(col("timestamp")))) \
                         .groupBy(
                            "date",
                            col("store_id"),
                            col("product_id"))
                         .agg(round(sum("revenue"), 3).alias("total_sales")))
                         
        logger.info("Computed Total Sales KPI.")
        return total_sales_df
    except Exception as e:
        logger.error(f"Failed to compute Total Sales KPI: {str(e)}")
        raise

def compute_stock_availability(inventory_df):
    """Compute Stock Availability (% of products above restock threshold)."""
    try:
        stock_availability_df = (
                                 inventory_df
                                 .withColumn("date", to_date(from_unixtime(col("last_updated")))) 
                                 .groupBy(
                                    "date",
                                    col("product_id"))
                                .agg(
                                    avg(when(col("stock_level") > coalesce(col("restock_threshold"), lit(0.0)), 1.0)
                                        .otherwise(0.0) * 100).alias("stock_availability")
                                ))
       
        logger.info("Computed Stock Availability KPI.")
        return stock_availability_df
    except Exception as e:
        logger.error(f"Failed to compute Stock Availability KPI: {str(e)}")
        raise

def compute_product_turnover(pos_df, inventory_df):
    """Compute Product Turnover Rate (units sold / avg stock level)."""
    try:
        # Aggregate units sold from POS
        units_sold_df = (
                         pos_df
                         .withColumn("date", to_date(from_unixtime(col("timestamp"))))\
                         .groupBy(
                            "date",
                            col("product_id"))
                        .agg(sum("quantity").alias("units_sold")))
        
        # Aggregate average stock level from Inventory
        avg_stock_df = (
                        inventory_df
                        .withColumn("date", to_date(from_unixtime(col("last_updated"))))\
                        .groupBy(
                            "date",
                            col("product_id"))
                      .agg(avg("stock_level").alias("avg_stock_level")))
        
        # Join and compute turnover rate
        turnover_df = (
                      units_sold_df
                      .join(avg_stock_df, ["date", "product_id"], "inner")
                      .withColumn("turnover_rate", round((col("units_sold") / col("avg_stock_level")),3))
                      .select("date", "product_id", "turnover_rate"))
        logger.info("Computed Product Turnover Rate KPI.")
        return turnover_df
    except Exception as e:
        logger.error(f"Failed to compute Product Turnover Rate KPI: {str(e)}")
        raise


def save_to_redshift(spark, df, jdbc_url, redshift_user, redshift_password, table_name):
    """Save DataFrame to Redshift table."""
    try:
        # Write to Redshift
        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", table_name) \
          .option("user", redshift_user) \
          .option("password", redshift_password) \
          .option("driver", "com.amazon.redshift.jdbc42.Driver") \
          .mode("overwrite") \
          .save()
        logger.info(f"Successfully saved KPIs to Redshift table {table_name}.")
    except Exception as e:
        logger.error(f"Failed to save to Redshift table {table_name}: {str(e)}")
        raise

def main():
    """Main function to orchestrate KPI computation and storage."""

    pos_path = args['POS_PATH']
    inventory_path = args['INVENTORY_PATH']
    redshift_jdbc_url = args['REDSHIFT_JDBC_URL']
    redshift_user = args['REDSHIFT_USER']
    redshift_password = args['REDSHIFT_PASSWORD']
    
    try:
        
        # Define schemas
        pos_schema, inventory_schema = define_schemas()
        
        # Read data
        pos_df, inventory_df = read_data(spark, pos_path, inventory_path, pos_schema, inventory_schema)
        
        # Compute KPIs
        total_sales_df = compute_total_sales(pos_df)
        stock_availability_df = compute_stock_availability(inventory_df)
        turnover_df = compute_product_turnover(pos_df, inventory_df)

        # Join all KPIs into a single DataFrame on date, store_id, and product_id
        kpi_df = total_sales_df.alias("sales") \
            .join(stock_availability_df.alias("stock"), 
                  on=["date", "product_id"], how="outer") \
            .join(turnover_df.alias("turnover"), 
                  on=["date", "product_id"], how="outer") \
            .select(
                col("date"),
                col("sales.store_id").alias("store_id"),
                col("product_id"),
                col("total_sales"),
                col("stock_availability"),
                col("turnover.turnover_rate").alias("product_turnover_rate")
            )
        
        # Drop rows where all KPIs are null
        kpi_df = kpi_df.filter(
            col("total_sales").isNotNull() |
            col("stock_availability").isNotNull() |
            col("product_turnover_rate").isNotNull()
        )
        
        logger.info("All KPIs combined into a single DataFrame without using kpi_name.")

        

        # Save to Redshift table
        # create_redshift_table(spark, redshift_jdbc_url, redshift_user, redshift_password)
        save_to_redshift(spark, kpi_df, redshift_jdbc_url, redshift_user, redshift_password, "sales_kpis")
        
        logger.info("Sales KPIs Glue job completed successfully.")
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        job.commit()
        spark.stop()
        logger.info("Spark session stopped.")
        

if __name__ == "__main__":
    main()
