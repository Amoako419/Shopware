# Import required libraries for AWS Glue
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, isnan, isnull, to_timestamp, lit, year, month, dayofmonth, dayofweek, mean, stddev, min, max, datediff, first
from pyspark.sql.types import DoubleType, TimestampType, IntegerType, LongType
from pyspark.sql.window import Window
from datetime import datetime
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'])

logger.info("Starting data ingestion...")

# Read from data catalogs
datasource1 = glueContext.create_dynamic_frame.from_catalog(
    database="database1",
    table_name="table1"
)
datasource2 = glueContext.create_dynamic_frame.from_catalog(
    database="database2", 
    table_name="table2"
)
datasource3 = glueContext.create_dynamic_frame.from_catalog(
    database="database3",
    table_name="table3"
)
datasource4 = glueContext.create_dynamic_frame.from_catalog(
    database="database4",
    table_name="table4"
)

logger.info("Converting to Spark DataFrames...")

# Convert dynamic frames to Spark DataFrames
df1 = datasource1.toDF()
df2 = datasource2.toDF()
df3 = datasource3.toDF()
df4 = datasource4.toDF()

# Cache frequently used dataframes
df1.cache()
df2.cache()
df3.cache()
df4.cache()

logger.info("Standardizing data types...")

# Standardize data types
dfs = [df1, df2, df3, df4]
for df in dfs:
    df = df.withColumn("value_column", col("value_column").cast(DoubleType()))
    df = df.withColumn("date_column", to_timestamp(col("date_column")))

logger.info("Cleaning data...")

# Remove duplicates from each dataframe
df1 = df1.dropDuplicates()
df2 = df2.dropDuplicates()
df3 = df3.dropDuplicates()
df4 = df4.dropDuplicates()

# Handle missing values - replace nulls with default values
required_columns = ["common_column", "value_column", "date_column"]
default_values = {
    "value_column": 0,
    "date_column": "1900-01-01 00:00:00"
}

df1 = df1.na.fill(default_values)
df2 = df2.na.fill(default_values)
df3 = df3.na.fill(default_values)
df4 = df4.na.fill(default_values)

# Enforce required columns exist in all dataframes
for df in dfs:
    for col_name in required_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

logger.info("Calculating data quality metrics...")

# Add data quality metrics calculations
for df in dfs:
    # Calculate completeness metrics
    total_rows = df.count()
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
    completeness_metrics = {c: 1 - (null_counts.collect()[0][c] / total_rows) for c in df.columns}
    
    # Calculate uniqueness metrics
    uniqueness_metrics = {c: df.select(c).distinct().count() / total_rows for c in df.columns}
    
    # Calculate value distribution metrics
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (DoubleType, IntegerType, LongType))]
    for col_name in numeric_cols:
        stats = df.select(
            mean(col(col_name)).alias('mean'),
            stddev(col(col_name)).alias('stddev'),
            min(col(col_name)).alias('min'),
            max(col(col_name)).alias('max')
        ).collect()[0]
        
        # Add statistical outlier detection
        df = df.withColumn(
            f"{col_name}_is_outlier",
            when(
                (col(col_name) > (stats['mean'] + 3 * stats['stddev'])) |
                (col(col_name) < (stats['mean'] - 3 * stats['stddev'])),
                True
            ).otherwise(False)
        )

logger.info("Adding data validation rules...")

# Add data validation rules
for df in dfs:
    # Add timestamp validation
    if "date_column" in df.columns:
        df = df.withColumn(
            "date_validation",
            when(
                (year(col("date_column")) < 1900) | 
                (col("date_column") > datetime.now()),
                "Invalid date"
            ).otherwise("Valid")
        )
    
    # Add numeric range validation
    if "value_column" in df.columns:
        df = df.withColumn(
            "value_validation",
            when(
                (col("value_column") < 0) |
                (col("value_column") > 1000000),  # Adjust threshold as needed
                "Out of range"
            ).otherwise("Valid")
        )

logger.info("Enriching with derived columns...")

# Add derived columns and enrichments
for df in dfs:
    if "date_column" in df.columns:
        # Extract date components
        df = df.withColumn("year", year(col("date_column")))
        df = df.withColumn("month", month(col("date_column")))
        df = df.withColumn("day", dayofmonth(col("date_column")))
        df = df.withColumn("day_of_week", dayofweek(col("date_column")))
        
        # Add time-based flags
        df = df.withColumn(
            "is_weekend",
            when(col("day_of_week").isin([1, 7]), True).otherwise(False)
        )
        
        # Calculate days since first event
        window_spec = Window.orderBy("date_column")
        df = df.withColumn(
            "days_since_first_event",
            datediff(col("date_column"), first(col("date_column")).over(window_spec))
        )

logger.info("Converting back to dynamic frame...")

# Get current timestamp for the folder name
current_timestamp = datetime.now().strftime('%Y%m%d')

# Convert each dataframe back to dynamic frame and write separately
df1_dynamic = DynamicFrame.fromDF(df1, glueContext, "df1")
df2_dynamic = DynamicFrame.fromDF(df2, glueContext, "df2") 
df3_dynamic = DynamicFrame.fromDF(df3, glueContext, "df3")
df4_dynamic = DynamicFrame.fromDF(df4, glueContext, "df4")

logger.info("Writing results to separate locations...")

# Write each dynamic frame to its own location
glueContext.write_dynamic_frame.from_options(
    frame=df1_dynamic,
    connection_type="s3",
    connection_options={
        "path": f"s3://output-bucket/dataset1/{current_timestamp}/"
    },
    format="parquet"
)

glueContext.write_dynamic_frame.from_options(
    frame=df2_dynamic,
    connection_type="s3",
    connection_options={
        "path": f"s3://output-bucket/dataset2/{current_timestamp}/"
    },
    format="parquet"
)

glueContext.write_dynamic_frame.from_options(
    frame=df3_dynamic,
    connection_type="s3",
    connection_options={
        "path": f"s3://output-bucket/dataset3/{current_timestamp}/"
    },
    format="parquet"
)

glueContext.write_dynamic_frame.from_options(
    frame=df4_dynamic,
    connection_type="s3",
    connection_options={
        "path": f"s3://output-bucket/dataset4/{current_timestamp}/"
    },
    format="parquet"
)

# Unpersist cached dataframes
df1.unpersist()
df2.unpersist()
df3.unpersist() 
df4.unpersist()

logger.info("Job completed successfully")

# Commit the job
job.commit()
