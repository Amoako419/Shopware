import base64
import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Redshift connection config via environment variables
# The REDSHIFT_CLUSTER_ID should be just the cluster identifier (name), not the full endpoint URL
REDSHIFT_CLUSTER_ID = (
    os.environ.get("REDSHIFT_CLUSTER_ID", "").split(":")[0]
    if ":" in os.environ.get("REDSHIFT_CLUSTER_ID", "")
    else os.environ.get("REDSHIFT_CLUSTER_ID", "")
)
REDSHIFT_DB = os.environ["REDSHIFT_DB"]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]


def lambda_handler(event, context):
    try:
        logger.info("Processing incoming Kinesis Data Stream records")
        records = []

        # Process records from Kinesis Data Streams format
        # Kinesis Data Streams event structure is different from Firehose
        for record in event["Records"]:
            # Decode and parse the data
            payload_bytes = base64.b64decode(record["kinesis"]["data"])
            payload = json.loads(payload_bytes.decode("utf-8"))
            records.append(payload)

        logger.info(f"Processed {len(records)} records from Kinesis Data Stream")

        # Load into DataFrames
        web_logs = pd.DataFrame([r for r in records if r.get("session_id")])
        crm_logs = pd.DataFrame([r for r in records if r.get("customer_id")])

        logger.info(f"Loaded {len(web_logs)} web logs and {len(crm_logs)} CRM logs")

        # Skip processing if no data
        if len(web_logs) == 0 and len(crm_logs) == 0:
            logger.info("No valid records to process")
            return {"statusCode": 200, "body": "No records to process"}

        # --- Transformation 1: Clean and enrich web logs ---
        logger.info("Starting web logs transformation")
        if not web_logs.empty:
            # Drop records with missing session_id or timestamp
            web_logs = web_logs.dropna(subset=["session_id", "timestamp"])
            # Convert timestamp to datetime
            web_logs["event_time"] = pd.to_datetime(web_logs["timestamp"], unit="s")
            logger.info(f"After cleaning, {len(web_logs)} web log records remain")
        else:
            logger.info("No web logs to process")

        # --- Transformation 2: Clean and enrich CRM logs ---
        logger.info("Starting CRM logs transformation")
        if not crm_logs.empty:
            # Drop records missing customer_id
            crm_logs = crm_logs.dropna(subset=["customer_id"])
            # Convert timestamp to datetime
            crm_logs["interaction_time"] = pd.to_datetime(
                crm_logs["timestamp"], unit="s"
            )
            logger.info(f"After cleaning, {len(crm_logs)} CRM log records remain")
        else:
            logger.info("No CRM logs to process")

        # --- KPI 1: Customer Engagement Score ---
        logger.info("Calculating Customer Engagement Score")
        customer_engagement_score = 0
        if (
            not crm_logs.empty
            and "interaction_type" in crm_logs.columns
            and "rating" in crm_logs.columns
        ):
            engagement_interactions = crm_logs[
                crm_logs["interaction_type"].isin(["Feedback", "Complaint"])
                & crm_logs["rating"].notna()
            ]
            if not engagement_interactions.empty:
                customer_engagement_score = engagement_interactions["rating"].mean()

        # --- KPI 2: Session Duration & Bounce Rate ---
        logger.info("Calculating Session Duration & Bounce Rate")
        avg_duration = 0
        bounce_rate = 0
        avg_duration_seconds = 0  # Initialize with default value

        if not web_logs.empty:
            session_group = web_logs.groupby("session_id").agg(
                {"timestamp": ["min", "max", "count"]}
            )
            session_group.columns = ["start_time", "end_time", "event_count"]
            session_group["session_duration"] = (
                session_group["end_time"] - session_group["start_time"]
            )
            session_group["is_bounce"] = (session_group["event_count"] == 1).astype(int)

            if not session_group.empty:
                avg_duration = session_group["session_duration"].mean()
                # Convert to float to handle both Timedelta and numpy.float64 types
                if hasattr(avg_duration, "total_seconds"):
                    # If it's a Timedelta object
                    avg_duration_seconds = avg_duration.total_seconds()
                else:
                    # If it's already a numeric value (numpy.float64)
                    avg_duration_seconds = (
                        float(avg_duration) if pd.notnull(avg_duration) else 0
                    )

                bounce_rate = (
                    (session_group["is_bounce"].sum() / len(session_group) * 100)
                    if len(session_group) > 0
                    else 0
                )

        # --- KPI 3: Loyalty Activity Rate ---
        logger.info("Calculating Loyalty Activity Rate")
        loyalty_rate = 0
        if not crm_logs.empty and "interaction_type" in crm_logs.columns:
            total_crm = len(crm_logs)
            loyalty_count = len(crm_logs[crm_logs["interaction_type"] == "Loyalty"])
            loyalty_rate = (loyalty_count / total_crm * 100) if total_crm > 0 else 0

        # Current timestamp for KPI recording
        kpi_ts = datetime.utcnow()
        kpi_date = kpi_ts.strftime("%Y-%m-%d %H:%M:%S")

        # --- Write to Redshift ---
        logger.info("Writing results to Redshift")
        insert_query = """
            INSERT INTO kpis (
                customer_engagement_score,
                avg_session_duration,
                bounce_rate_percent,
                loyalty_activity_rate_percent,
                kpi_date
            ) VALUES (:1, :2, :3, :4, :5)
        """

        try:
            # Log the cluster ID being used
            logger.info(f"Using Redshift Cluster ID: {REDSHIFT_CLUSTER_ID}")

            # Create parameters list with both name and value for each parameter
            parameters_list = [
                {"name": "1", "value": str(customer_engagement_score or 0)},
                {"name": "2", "value": str(avg_duration_seconds or 0)},
                {"name": "3", "value": str(bounce_rate or 0)},
                {"name": "4", "value": str(loyalty_rate or 0)},
                {"name": "5", "value": kpi_date},
            ]

            # Check if we need to parse the endpoint format
            if REDSHIFT_CLUSTER_ID and (
                "." in REDSHIFT_CLUSTER_ID or ":" in REDSHIFT_CLUSTER_ID
            ):
                # Extract just the cluster name from endpoint format
                # Format could be: cluster-name.identifier.region.redshift.amazonaws.com:port
                cluster_name = REDSHIFT_CLUSTER_ID.split(".")[0]
                logger.info(f"Extracted cluster name from endpoint: {cluster_name}")

                redshift_client = boto3.client("redshift-data")
                response = redshift_client.execute_statement(
                    ClusterIdentifier=cluster_name,
                    Database=REDSHIFT_DB,
                    DbUser=REDSHIFT_USER,
                    Sql=insert_query,
                    Parameters=parameters_list,
                )
            else:
                redshift_client = boto3.client("redshift-data")
                response = redshift_client.execute_statement(
                    ClusterIdentifier=REDSHIFT_CLUSTER_ID,
                    Database=REDSHIFT_DB,
                    DbUser=REDSHIFT_USER,
                    Sql=insert_query,
                    Parameters=parameters_list,
                )
            logger.info(
                f"Successfully wrote KPIs to Redshift. QueryId: {response.get('Id', 'N/A')}"
            )

            # Optionally, store raw data in tables too
            if len(web_logs) > 0:
                logger.info(f"Storing {len(web_logs)} web logs in Redshift")
                # Add code here to store web_logs in a Redshift table

            if len(crm_logs) > 0:
                logger.info(f"Storing {len(crm_logs)} CRM logs in Redshift")
                # Add code here to store crm_logs in a Redshift table

            return {"statusCode": 200, "body": "KPIs and data written to Redshift"}
        except Exception as e:
            logger.error(f"Failed to write to Redshift: {str(e)}")
            raise

    except Exception as e:
        logger.error(f"Error processing records: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": "Failed to process records", "details": str(e)}
            ),
        }
