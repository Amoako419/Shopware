# Import required libraries for AWS Glue
import logging
import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    datediff,
    dayofmonth,
    dayofweek,
    first,
    isnan,
    isnull,
    lit,
    max,
    mean,
    min,
    month,
    stddev,
    to_timestamp,
    when,
    year,
)
from pyspark.sql.types import DoubleType, IntegerType, LongType, TimestampType
from pyspark.sql.window import Window

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET_NAME",
        "DATABASE_NAME",
        "WEB_LOGS_TABLE_NAME",
        "CRM_LOGS_TABLE_NAME",
        "POS_TABLE_NAME",
        "INVENTORY_TABLE_NAME",
    ],
)
job.init(args["JOB_NAME"])

S3_BUCKET_NAME = args["S3_BUCKET_NAME"]
DATABASE_NAME = args["DATABASE_NAME"]

WEB_LOGS_TABLE_NAME = args["WEB_LOGS_TABLE_NAME"]
CRM_LOGS_TABLE_NAME = args["CRM_LOGS_TABLE_NAME"]
POS_TABLE_NAME = args["POS_TABLE_NAME"]
INVENTORY_TABLE_NAME = args["INVENTORY_TABLE_NAME"]


logger.info("Starting data ingestion...")

# Timestamp for folder naming
current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Read from data catalogs
# datasource1 = glueContext.create_dynamic_frame.from_catalog(
#     database=DATABASE_NAME,
#     table_name=WEB_LOGS_TABLE_NAME
# )
# datasource2 = glueContext.create_dynamic_frame.from_catalog(
#     database=DATABASE_NAME,
#     table_name=CRM_LOGS_TABLE_NAME
# )
datasource3 = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME, table_name=POS_TABLE_NAME
)
datasource4 = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME, table_name=INVENTORY_TABLE_NAME
)

# Convert dynamic frames to Spark DataFrames
# df1 = datasource1.toDF()
# df2 = datasource2.toDF()
df3 = datasource3.toDF()
df4 = datasource4.toDF()

# Write each dynamic frame to its own location
# glueContext.write_dynamic_frame.from_options(
#     frame=datasource1,
#     connection_type="s3",
#     connection_options={
#         "path": f"s3://{S3_BUCKET_NAME}/silver-web-logs/{current_timestamp}/"
#     },
#     format="parquet"
# )
# glueContext.write_dynamic_frame.from_options(
#     frame=datasource2,
#     connection_type="s3",
#     connection_options={
#         "path": f"s3://{S3_BUCKET_NAME}/silver-crm-logs/{current_timestamp}/"
#     },
#     format="parquet"
# )
glueContext.write_dynamic_frame.from_options(
    frame=datasource3,
    connection_type="s3",
    connection_options={
        "path": f"s3://{S3_BUCKET_NAME}/silver-pos/{current_timestamp}/"
    },
    format="parquet",
)
glueContext.write_dynamic_frame.from_options(
    frame=datasource4,
    connection_type="s3",
    connection_options={
        "path": f"s3://{S3_BUCKET_NAME}/silver-inventory/{current_timestamp}/"
    },
    format="parquet",
)

# Commit the job
job.commit()
