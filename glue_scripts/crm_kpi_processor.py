import json
import logging
import sys
from datetime import datetime

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_unixtime, max, min
from pyspark.sql.types import TimestampType

# Configure logging to CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def setup_spark_context():
    """Initialize Spark and Glue context."""
    try:
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        # Optimize for near real-time processing
        spark.conf.set("spark.sql.shuffle.partitions", "10")
        spark.conf.set("spark.default.parallelism", "10")
        return glue_context, spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark context: {str(e)}")
        raise


def get_job_parameters():
    """Retrieve job parameters."""
    try:
        args = getResolvedOptions(
            sys.argv, ["JOB_NAME", "database_name", "table_name", "output_bucket"]
        )
        return {
            "job_name": args["JOB_NAME"],
            "database_name": args["database_name"],
            "table_name": args["table_name"],
            "output_bucket": args["output_bucket"],
        }
    except Exception as e:
        logger.error(f"Failed to retrieve job parameters: {str(e)}")
        raise


def read_data_from_catalog(glue_context, database_name, table_name):
    """Read data from AWS Glue Data Catalog."""
    try:
        logger.info(f"Reading data from {database_name}.{table_name}")
        dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
            database=database_name, table_name=table_name, transformation_ctx="crm_logs"
        )
        df = dynamic_frame.toDF()
        logger.info(f"Successfully read {df.count()} records")
        return df
    except Exception as e:
        logger.error(f"Failed to read data from catalog: {str(e)}")
        raise


def compute_feedback_score(df):
    """Compute average feedback score."""
    try:
        feedback_df = df.filter(col("interaction_type") == "Feedback")
        feedback_score = feedback_df.agg(
            avg("rating").alias("avg_feedback_score")
        ).collect()[0]["avg_feedback_score"]
        feedback_score = feedback_score if feedback_score is not None else 0.0
        logger.info(f"Computed Feedback Score: {feedback_score}")
        return feedback_score
    except Exception as e:
        logger.error(f"Failed to compute feedback score: {str(e)}")
        raise


def compute_interaction_volume_by_type(df):
    """Compute interaction volume by type."""
    try:
        volume_df = df.groupBy("interaction_type").agg(count("*").alias("volume"))
        volume_dict = {
            row["interaction_type"]: row["volume"] for row in volume_df.collect()
        }
        logger.info(f"Interaction Volume by Type: {volume_dict}")
        return volume_dict
    except Exception as e:
        logger.error(f"Failed to compute interaction volume: {str(e)}")
        raise


def compute_time_to_resolution(df):
    """Compute average time-to-resolution for complaints."""
    try:
        complaint_df = df.filter(col("interaction_type") == "Complaint")
        complaint_df = complaint_df.withColumn(
            "timestamp_dt", from_unixtime(col("timestamp")).cast(TimestampType())
        )
        time_diff = complaint_df.groupBy("customer_id").agg(
            (max("timestamp") - min("timestamp")).cast("long").alias("resolution_time")
        )
        avg_resolution = time_diff.agg(
            avg("resolution_time").alias("avg_resolution_time")
        ).collect()[0]["avg_resolution_time"]
        avg_resolution = avg_resolution if avg_resolution is not None else 0.0
        logger.info(f"Average Time-to-Resolution: {avg_resolution} seconds")
        return avg_resolution
    except Exception as e:
        logger.error(f"Failed to compute time-to-resolution: {str(e)}")
        raise


def write_kpis_to_s3(spark, kpis, output_path):
    """Write KPIs to S3 in Delta Lake format."""
    try:
        logger.info(f"Writing KPIs to {output_path}")
        kpi_df = spark.createDataFrame(
            [kpis],
            schema=[
                "feedback_score",
                "interaction_volume",
                "time_to_resolution",
                "timestamp",
            ],
        )
        kpi_df.write.format("delta").mode("append").save(output_path)
        logger.info("Successfully wrote KPIs to S3")
    except Exception as e:
        logger.error(f"Failed to write KPIs to S3: {str(e)}")
        raise


def main():
    """Main function to orchestrate KPI computation."""
    try:
        # Initialize Spark and Glue
        glue_context, spark = setup_spark_context()

        # Get job parameters
        params = get_job_parameters()
        database_name = params["database_name"]
        table_name = params["table_name"]
        output_bucket = params["output_bucket"]
        output_path = f"s3://{output_bucket}/crm-kpis/"

        # Read data
        df = read_data_from_catalog(glue_context, database_name, table_name)

        # Compute KPIs
        feedback_score = compute_feedback_score(df)
        interaction_volume = compute_interaction_volume_by_type(df)
        time_to_resolution = compute_time_to_resolution(df)

        # Prepare KPI output
        kpis = {
            "feedback_score": feedback_score,
            "interaction_volume": json.dumps(interaction_volume),
            "time_to_resolution": time_to_resolution,
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Write to S3
        write_kpis_to_s3(spark, kpis, output_path)

        # Initialize and commit job
        job = Job(glue_context)
        job.init(params["job_name"], params)
        job.commit()
        logger.info("Job completed successfully")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
