
# Import required libraries for AWS Glue
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, isnan, isnull

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'])

# Read from first data catalog
datasource1 = glueContext.create_dynamic_frame.from_catalog(
    database="database1",
    table_name="table1"
)

# Read from second data catalog
datasource2 = glueContext.create_dynamic_frame.from_catalog(
    database="database2", 
    table_name="table2"
)

# Read from third data catalog
datasource3 = glueContext.create_dynamic_frame.from_catalog(
    database="database3",
    table_name="table3"
)

# Read from fourth data catalog
datasource4 = glueContext.create_dynamic_frame.from_catalog(
    database="database4",
    table_name="table4"
)

# Convert dynamic frames to Spark DataFrames
df1 = datasource1.toDF()
df2 = datasource2.toDF()
df3 = datasource3.toDF()
df4 = datasource4.toDF()

# Remove duplicates from each dataframe
df1 = df1.dropDuplicates()
df2 = df2.dropDuplicates()
df3 = df3.dropDuplicates()
df4 = df4.dropDuplicates()

# Handle missing values - replace nulls with default values
required_columns = ["common_column", "value_column", "date_column"]
df1 = df1.fillna({
    "value_column": 0,
    "date_column": "1900-01-01"
})
df2 = df2.fillna({
    "value_column": 0,
    "date_column": "1900-01-01"
})
df3 = df3.fillna({
    "value_column": 0,
    "date_column": "1900-01-01"
})
df4 = df4.fillna({
    "value_column": 0,
    "date_column": "1900-01-01"
})

# Enforce required columns exist in all dataframes
for df in [df1, df2, df3, df4]:
    for col_name in required_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

# Join all dataframes
combined_df = df1.join(df2, ["common_column"], "inner") \
                 .join(df3, ["common_column"], "inner") \
                 .join(df4, ["common_column"], "inner")

# Additional data quality checks
combined_df = combined_df.filter(col("value_column") >= 0) \
                        .filter(col("date_column").isNotNull())

# Convert back to dynamic frame
combined_dynamic_frame = DynamicFrame.fromDF(combined_df, glueContext, "combined")

# Write the results
glueContext.write_dynamic_frame.from_options(
    frame=combined_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://output-bucket/path"
    },
    format="parquet"
)

# Commit the job
job.commit()
