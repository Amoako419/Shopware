import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    date_format,
    from_unixtime,
    lit,
    max,
    min,
    sum,
    to_timestamp,
    when,
)

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Glue context
logger.info("Initializing Glue job")
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "redshift_url",
        "redshift_database",
        "redshift_user",
        "redshift_password",
        "redshift_table",
    ],
)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load data
logger.info("Loading data from Glue catalog")
web_logs = glueContext.create_dynamic_frame.from_catalog(
    database="api-data-db", table_name="web-logs"
).toDF()
logger.info(f"Loaded {web_logs.count()} web log records")

crm_logs = glueContext.create_dynamic_frame.from_catalog(
    database="api-data-db", table_name="crm-logs"
).toDF()
logger.info(f"Loaded {crm_logs.count()} CRM log records")

# --- Transformation 1: Clean and enrich web logs ---
logger.info("Starting web logs transformation")
# Drop records with missing session_id or timestamp
web_logs = web_logs.filter(
    (col("session_id").isNotNull()) & (col("timestamp").isNotNull())
)
logger.info(f"After cleaning, {web_logs.count()} web log records remain")

# Convert timestamp to readable format
web_logs = web_logs.withColumn("event_time", from_unixtime(col("timestamp"))).cache()

# --- Transformation 2: Clean and enrich CRM logs ---
logger.info("Starting CRM logs transformation")
# Drop records missing customer_id
crm_logs = crm_logs.filter(col("customer_id").isNotNull())
logger.info(f"After cleaning, {crm_logs.count()} CRM log records remain")

# Convert timestamp to readable format
crm_logs = crm_logs.withColumn(
    "interaction_time", from_unixtime(col("timestamp"))
).cache()

# --- KPI 1: Customer Engagement Score ---
logger.info("Calculating Customer Engagement Score")
engagement_interactions = crm_logs.filter(
    (col("interaction_type").isin("Feedback", "Complaint")) & col("rating").isNotNull()
).cache()

engagement_score_df = engagement_interactions.agg(
    avg("rating").alias("customer_engagement_score")
)

# --- KPI 2: Session Duration & Bounce Rate ---
logger.info("Calculating Session Duration & Bounce Rate")
session_activity = (
    web_logs.groupBy("session_id")
    .agg(
        min("timestamp").alias("start_time"),
        max("timestamp").alias("end_time"),
        count("*").alias("event_count"),
    )
    .withColumn("session_duration", col("end_time") - col("start_time"))
    .withColumn("is_bounce", when(col("event_count") == 1, 1).otherwise(0))
    .cache()
)

session_metrics_df = session_activity.agg(
    avg("session_duration").alias("avg_session_duration"),
    (100 * (sum("is_bounce") / count("*"))).alias("bounce_rate_percent"),
)

# --- KPI 3: Loyalty Activity Rate ---
logger.info("Calculating Loyalty Activity Rate")
loyalty_total = crm_logs.count()
loyalty_activity = crm_logs.filter(col("interaction_type") == "Loyalty").count()
loyalty_activity_rate = (
    (loyalty_activity / loyalty_total) * 100 if loyalty_total > 0 else 0
)
loyalty_df = spark.createDataFrame(
    [(loyalty_activity_rate,)], ["loyalty_activity_rate_percent"]
)

# --- Combine KPIs ---
logger.info("Combining KPI metrics")
final_kpis_df = engagement_score_df.crossJoin(session_metrics_df).crossJoin(loyalty_df)

# --- Enrich with metadata ---
final_kpis_df = final_kpis_df.withColumn(
    "kpi_date", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")
)


# Write to Redshift
logger.info("Writing results to Redshift")
redshift_url = f"jdbc:redshift://{args['redshift_url']}/{args['redshift_database']}"
final_kpis_df.write.format("jdbc").option("url", redshift_url).option(
    "dbtable", args["redshift_table"]
).option("user", args["redshift_user"]).option(
    "password", args["redshift_password"]
).mode(
    "append"
).save()

# Unpersist cached DataFrames
web_logs.unpersist()
crm_logs.unpersist()
engagement_interactions.unpersist()
session_activity.unpersist()

logger.info("Job completed successfully")
job.commit()
