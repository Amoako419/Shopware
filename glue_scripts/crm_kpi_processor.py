import sys
import time
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import logging
from delta import configure_spark_with_delta_pip

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def read_data(glue_context, database_name, table_name):
    """
    Read CRM logs from the Glue Data Catalog
    
    Args:
        glue_context: The GlueContext object
        database_name: Name of the database in the Glue Data Catalog
        table_name: Name of the table in the Glue Data Catalog
        
    Returns:
        DataFrame: The CRM logs data
    """
    try:
        logger.info(f"Reading data from {database_name}.{table_name}")
        
        # Read from Glue Data Catalog
        dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name
        )
        
        # Convert to DataFrame for easier processing
        df = dynamic_frame.toDF()
        
        # Convert epoch timestamp to timestamp type for easier processing
        df = df.withColumn("timestamp_dt", F.from_unixtime(F.col("timestamp")).cast(TimestampType()))
        
        logger.info(f"Successfully read {df.count()} records")
        return df
        
    except Exception as e:
        logger.error(f"Error reading data: {str(e)}")
        raise

def compute_feedback_score(df):
    """
    Compute average feedback score by interaction type and channel
    
    Args:
        df: DataFrame containing CRM logs
        
    Returns:
        DataFrame: Computed feedback scores
    """
    try:
        logger.info("Computing feedback scores")
        
        # Filter for non-null ratings
        feedback_df = df.filter(df.rating.isNotNull())
        
        # Compute average rating by interaction type and channel
        feedback_scores = feedback_df.groupBy("interaction_type", "channel") \
            .agg(
                F.avg("rating").alias("avg_rating"),
                F.count("rating").alias("rating_count"),
                F.min("rating").alias("min_rating"),
                F.max("rating").alias("max_rating")
            )
        
        # Add overall average
        overall_avg = feedback_df.agg(
            F.lit("Overall").alias("interaction_type"),
            F.lit(None).alias("channel"),
            F.avg("rating").alias("avg_rating"),
            F.count("rating").alias("rating_count"),
            F.min("rating").alias("min_rating"),
            F.max("rating").alias("max_rating")
        )
        
        feedback_scores = feedback_scores.union(overall_avg)
        
        # Add processing timestamp
        feedback_scores = feedback_scores.withColumn(
            "processed_at", 
            F.current_timestamp()
        )
        
        logger.info(f"Computed feedback scores with {feedback_scores.count()} records")
        return feedback_scores
        
    except Exception as e:
        logger.error(f"Error computing feedback scores: {str(e)}")
        raise

def compute_interaction_volume(df):
    """
    Compute interaction volume by type and channel
    
    Args:
        df: DataFrame containing CRM logs
        
    Returns:
        DataFrame: Computed interaction volumes
    """
    try:
        logger.info("Computing interaction volumes")
        
        # Group by interaction type, channel, and date
        df = df.withColumn("interaction_date", F.to_date("timestamp_dt"))
        
        interaction_volumes = df.groupBy(
            "interaction_type", 
            "channel", 
            "interaction_date"
        ).count().withColumnRenamed("count", "volume")
        
        # Add totals by date
        date_totals = df.groupBy("interaction_date") \
            .count() \
            .withColumn("interaction_type", F.lit("All")) \
            .withColumn("channel", F.lit(None)) \
            .withColumnRenamed("count", "volume")
        
        # Add totals by type
        type_totals = df.groupBy("interaction_type", "interaction_date") \
            .count() \
            .withColumn("channel", F.lit(None)) \
            .withColumnRenamed("count", "volume")
        
        # Combine all results
        interaction_volumes = interaction_volumes.union(date_totals).union(type_totals)
        
        # Add processing timestamp
        interaction_volumes = interaction_volumes.withColumn(
            "processed_at", 
            F.current_timestamp()
        )
        
        logger.info(f"Computed interaction volumes with {interaction_volumes.count()} records")
        return interaction_volumes
        
    except Exception as e:
        logger.error(f"Error computing interaction volumes: {str(e)}")
        raise

def compute_time_to_resolution(df):
    """
    Compute time-to-resolution metrics
    
    Args:
        df: DataFrame containing CRM logs
        
    Returns:
        DataFrame: Computed time-to-resolution metrics
    """
    try:
        logger.info("Computing time-to-resolution metrics")
        
        # For this KPI, we need to identify when an interaction is "resolved"
        # Assuming a complaint is resolved when there's a feedback after it
        # This is a simplified approach - in production, you'd need clear resolution criteria
        
        # First, let's identify customer journeys
        window_spec = Window.partitionBy("customer_id").orderBy("timestamp")
        
        journey_df = df.withColumn(
            "next_interaction_type", 
            F.lead("interaction_type").over(window_spec)
        ).withColumn(
            "next_timestamp", 
            F.lead("timestamp").over(window_spec)
        )
        
        # Filter for complaints that were followed by feedback
        resolution_df = journey_df.filter(
            (F.col("interaction_type") == "Complaint") & 
            (F.col("next_interaction_type") == "Feedback")
        )
        
        # Calculate resolution time in minutes
        resolution_df = resolution_df.withColumn(
            "resolution_time_minutes", 
            (F.col("next_timestamp") - F.col("timestamp")) / 60
        )
        
        # Aggregate by channel
        time_to_resolution = resolution_df.groupBy("channel") \
            .agg(
                F.avg("resolution_time_minutes").alias("avg_resolution_time_minutes"),
                F.min("resolution_time_minutes").alias("min_resolution_time_minutes"),
                F.max("resolution_time_minutes").alias("max_resolution_time_minutes"),
                F.count("resolution_time_minutes").alias("resolved_complaint_count")
            )
        
        # Add overall average
        overall_avg = resolution_df.agg(
            F.lit(None).alias("channel"),
            F.avg("resolution_time_minutes").alias("avg_resolution_time_minutes"),
            F.min("resolution_time_minutes").alias("min_resolution_time_minutes"),
            F.max("resolution_time_minutes").alias("max_resolution_time_minutes"),
            F.count("resolution_time_minutes").alias("resolved_complaint_count")
        )
        
        time_to_resolution = time_to_resolution.union(overall_avg)
        
        # Add processing timestamp
        time_to_resolution = time_to_resolution.withColumn(
            "processed_at", 
            F.current_timestamp()
        )
        
        logger.info(f"Computed time-to-resolution with {time_to_resolution.count()} records")
        return time_to_resolution
        
    except Exception as e:
        logger.error(f"Error computing time-to-resolution: {str(e)}")
        raise

def write_to_delta(df, output_path):
    """
    Write DataFrame to S3 in Delta Lake format
    
    Args:
        df: DataFrame to write
        output_path: S3 path to write to
    """
    try:
        logger.info(f"Writing data to {output_path}")
        
        # Write to Delta Lake format
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(output_path)
        
        logger.info(f"Successfully wrote data to {output_path}")
        
    except Exception as e:
        logger.error(f"Error writing data to {output_path}: {str(e)}")
        raise

def main():
    """
    Main entry point for the Glue job
    """
    try:
        start_time = time.time()
        logger.info("Starting CRM KPI processing")
        
        # Initialize Spark and Glue contexts
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        
        # Get job parameters
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'database_name',
            'table_name',
            'output_bucket'
        ])
        
        job.init(args['JOB_NAME'], args)
        
        # Set Spark configurations for better performance
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.broadcastTimeout", "3600")
        
        # Configure Delta Lake
        configure_spark_with_delta_pip(spark)
        
        # Extract parameters
        database_name = args.get('database_name', 'api-data-db')
        table_name = args.get('table_name', 'crm_logs')
        output_bucket = args.get('output_bucket', 'shopware-gold-layer-125/crm-kpis')
        
        logger.info(f"Processing with parameters: database={database_name}, "
                   f"table={table_name}, output_bucket={output_bucket}")
        
        # Read data
        df = read_data(glue_context, database_name, table_name)
        
        # Compute KPIs
        feedback_scores = compute_feedback_score(df)
        interaction_volumes = compute_interaction_volume(df)
        time_to_resolution = compute_time_to_resolution(df)
        
        # Write results to S3 in Delta Lake format
        write_to_delta(feedback_scores, f"s3://{output_bucket}/feedback_scores")
        write_to_delta(interaction_volumes, f"s3://{output_bucket}/interaction_volumes")
        write_to_delta(time_to_resolution, f"s3://{output_bucket}/time_to_resolution")
        
        end_time = time.time()
        logger.info(f"CRM KPI processing completed in {end_time - start_time:.2f} seconds")
        
        # Commit the job
        job.commit()
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()