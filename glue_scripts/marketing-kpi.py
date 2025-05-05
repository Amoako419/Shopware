import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, count, avg, max, min, when, sum, lit, to_timestamp, from_unixtime
from pyspark.sql.functions import current_timestamp, date_format

# Glue context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load data
web_logs = glueContext.create_dynamic_frame.from_catalog(
    database="api-data-db",
    table_name="web-logs"
).toDF()

crm_logs = glueContext.create_dynamic_frame.from_catalog(
    database="api-data-db", 
    table_name="crm-logs"
).toDF()

# --- Transformation 1: Clean and enrich web logs ---
# Drop records with missing session_id or timestamp
web_logs = web_logs.filter((col("session_id").isNotNull()) & (col("timestamp").isNotNull()))

# Convert timestamp to readable format
web_logs = web_logs.withColumn("event_time", from_unixtime(col("timestamp")))

# --- Transformation 2: Clean and enrich CRM logs ---
# Drop records missing customer_id
crm_logs = crm_logs.filter(col("customer_id").isNotNull())

# Convert timestamp to readable format
crm_logs = crm_logs.withColumn("interaction_time", from_unixtime(col("timestamp")))

# --- KPI 1: Customer Engagement Score ---
engagement_interactions = crm_logs.filter(
    (col("interaction_type").isin("Feedback", "Complaint")) & col("rating").isNotNull()
)

engagement_score_df = engagement_interactions.agg(
    avg("rating").alias("customer_engagement_score")
)

# --- KPI 2: Session Duration & Bounce Rate ---
session_activity = web_logs.groupBy("session_id") \
    .agg(min("timestamp").alias("start_time"),
         max("timestamp").alias("end_time"),
         count("*").alias("event_count")) \
    .withColumn("session_duration", col("end_time") - col("start_time")) \
    .withColumn("is_bounce", when(col("event_count") == 1, 1).otherwise(0))

session_metrics_df = session_activity.agg(
    avg("session_duration").alias("avg_session_duration"),
    (100 * (sum("is_bounce") / count("*"))).alias("bounce_rate_percent")
)

# --- KPI 3: Loyalty Activity Rate ---
loyalty_total = crm_logs.count()
loyalty_activity = crm_logs.filter(col("interaction_type") == "Loyalty").count()
loyalty_activity_rate = (loyalty_activity / loyalty_total) * 100 if loyalty_total > 0 else 0
loyalty_df = spark.createDataFrame([(loyalty_activity_rate,)], ["loyalty_activity_rate_percent"])

# --- Combine KPIs ---
final_kpis_df = engagement_score_df.crossJoin(session_metrics_df).crossJoin(loyalty_df)

# --- Enrich with metadata ---
final_kpis_df = final_kpis_df.withColumn("kpi_date", lit(current_date()))

# --- Save to S3 ---
# --- Save as Delta Table ---
# Add partition column with formatted timestamp
final_kpis_df = final_kpis_df.withColumn("kpi_ts", date_format(current_timestamp(), "yyyy-MM-dd_HH-mm"))

# Write as Delta with partitioning
final_kpis_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("kpi_ts") \
    .save("s3://shopware-gold-layer-125/market-kpis/")


job.commit()
