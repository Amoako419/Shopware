import json
import pandas as pd
import boto3
from datetime import datetime
import os
import base64
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Redshift connection config via environment variables
REDSHIFT_CLUSTER_ID = os.environ.get('REDSHIFT_CLUSTER_ID', '').split(':')[0] if ':' in os.environ.get('REDSHIFT_CLUSTER_ID', '') else os.environ.get('REDSHIFT_CLUSTER_ID', '')
REDSHIFT_DB = os.environ['REDSHIFT_DB']
REDSHIFT_USER = os.environ['REDSHIFT_USER']

def lambda_handler(event, context):
    try:
        start_time = datetime.utcnow()
        logger.info(f"Starting Lambda execution at {start_time.isoformat()}")

        # Log event structure
        logger.info(f"Received event with {len(event.get('Records', []))} records")
        if not event.get('Records'):
            logger.warning("No records found in event")
            return {'statusCode': 200, 'body': 'No records to process'}

        logger.info("Processing incoming Kinesis Data Stream records")
        records = []

        # Process records from Kinesis Data Streams
        for i, record in enumerate(event['Records']):
            try:
                # Decode and parse the data
                payload_bytes = base64.b64decode(record['kinesis']['data'])
                payload = json.loads(payload_bytes.decode('utf-8'))
                records.append(payload)
                # Log sample of record (avoid sensitive data)
                logger.debug(f"Record {i+1} sample: customer_id={payload.get('customer_id', 'N/A')}, "
                            f"interaction_type={payload.get('interaction_type', 'N/A')}, "
                            f"timestamp={payload.get('timestamp', 'N/A')}")
            except Exception as e:
                logger.error(f"Failed to process record {i+1}: {str(e)}")
                continue
            
        logger.info(f"Processed {len(records)} valid records from Kinesis Data Stream")

        # Load into DataFrame (only Customer Interactions data)
        crm_logs = pd.DataFrame([r for r in records if r.get('customer_id')])
        
        logger.info(f"Loaded {len(crm_logs)} CRM logs into DataFrame")
        if not crm_logs.empty:
            logger.debug(f"CRM logs columns: {list(crm_logs.columns)}")
            logger.debug(f"Sample CRM log: {crm_logs.iloc[0].to_dict() if len(crm_logs) > 0 else 'N/A'}")
        else:
            logger.info("No valid CRM logs to process")
            return {'statusCode': 200, 'body': 'No CRM logs to process'}

        # --- Transformation: Clean and enrich CRM logs ---
        logger.info("Starting CRM logs transformation")
        transform_start = datetime.utcnow()
        if not crm_logs.empty:
            # Drop records missing customer_id or timestamp
            original_count = len(crm_logs)
            crm_logs = crm_logs.dropna(subset=['customer_id', 'timestamp'])
            logger.info(f"Dropped {original_count - len(crm_logs)} records missing customer_id or timestamp")
            
            # Convert timestamp to datetime
            crm_logs['interaction_time'] = pd.to_datetime(crm_logs['timestamp'], unit='s')
            logger.debug(f"Converted timestamps to datetime; sample interaction_time: {crm_logs['interaction_time'].iloc[0] if len(crm_logs) > 0 else 'N/A'}")
            
            # Ensure rating is numeric and within 1-5
            if 'rating' in crm_logs.columns:
                crm_logs['rating'] = pd.to_numeric(crm_logs['rating'], errors='coerce')
                valid_ratings = crm_logs['rating'].between(1, 5, inclusive='both')
                invalid_rating_count = len(crm_logs[~valid_ratings & crm_logs['rating'].notna()])
                crm_logs = crm_logs[valid_ratings | crm_logs['rating'].isna()]
                logger.info(f"Filtered {invalid_rating_count} records with invalid ratings (not 1-5)")
            logger.info(f"After cleaning, {len(crm_logs)} CRM log records remain")
            logger.info(f"Transformation completed in {(datetime.utcnow() - transform_start).total_seconds()} seconds")
        else:
            logger.info("No CRM logs to transform")

        # --- KPI 1: Feedback Score ---
        logger.info("Calculating Feedback Score")
        feedback_score = 0
        if not crm_logs.empty and 'rating' in crm_logs.columns:
            feedback_ratings = crm_logs[crm_logs['rating'].notna()]
            logger.info(f"Found {len(feedback_ratings)} records with valid ratings")
            if not feedback_ratings.empty:
                feedback_score = feedback_ratings['rating'].mean()
                logger.info(f"Calculated Feedback Score: {feedback_score:.2f}")
            else:
                logger.info("No valid ratings for Feedback Score calculation")

        # --- KPI 2: Interaction Volume by Type ---
        logger.info("Calculating Interaction Volume by Type")
        interaction_volume = {}
        if not crm_logs.empty and 'interaction_type' in crm_logs.columns:
            volume_by_type = crm_logs['interaction_type'].value_counts().to_dict()
            interaction_volume = volume_by_type
            logger.info(f"Interaction Volume by Type: {interaction_volume}")
        else:
            logger.info("No interaction_type data for Interaction Volume calculation")

        # --- KPI 3: Time-to-Resolution ---
        logger.info("Calculating Time-to-Resolution")
        avg_time_to_resolution = 0
        if not crm_logs.empty and 'interaction_type' in crm_logs.columns:
            resolution_interactions = crm_logs[crm_logs['interaction_type'] == 'Resolution']
            logger.info(f"Found {len(resolution_interactions)} Resolution interactions")
            if not resolution_interactions.empty:
                sorted_logs = crm_logs.sort_values(['customer_id', 'timestamp'])
                sorted_logs['prev_interaction_time'] = sorted_logs.groupby('customer_id')['interaction_time'].shift(1)
                resolution_times = sorted_logs[sorted_logs['interaction_type'] == 'Resolution']
                resolution_times = resolution_times[resolution_times['prev_interaction_time'].notna()]
                logger.info(f"Found {len(resolution_times)} valid Resolution interactions with prior interactions")
                if not resolution_times.empty:
                    resolution_times['time_to_resolve'] = (resolution_times['interaction_time'] - resolution_times['prev_interaction_time']).dt.total_seconds() / 60.0
                    avg_time_to_resolution = resolution_times['time_to_resolve'].mean()
                    logger.info(f"Calculated Avg Time-to-Resolution: {avg_time_to_resolution:.2f} minutes")
                else:
                    logger.info("No valid Resolution interactions for Time-to-Resolution calculation")
            else:
                logger.info("No Resolution interactions for Time-to-Resolution calculation")

        # Current timestamp for KPI recording
        kpi_ts = datetime.utcnow()
        kpi_date = kpi_ts.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"KPI timestamp: {kpi_date}")

        # --- Write to Redshift ---
        logger.info("Writing results to Redshift")
        redshift_start = datetime.utcnow()
        interaction_volume_json = json.dumps(interaction_volume)
        logger.debug(f"Serialized interaction_volume: {interaction_volume_json}")
        
        insert_query = """
            INSERT INTO customer_support_kpis (
                feedback_score,
                interaction_volume_by_type,
                avg_time_to_resolution_minutes,
                kpi_date
            ) VALUES (:1, :2, :3, :4)
        """

        try:
            logger.info(f"Using Redshift Cluster ID: {REDSHIFT_CLUSTER_ID}, Database: {REDSHIFT_DB}, User: {REDSHIFT_USER}")
            
            parameters_list= [
                {'name': '1', 'value': str(feedback_score or 0)},
                {'name': '2', 'value': interaction_volume_json or '{}'},
                {'name': '3', 'value': str(avg_time_to_resolution or 0)},
                {'name': '4', 'value': kpi_date}
            ]
            logger.debug(f"Redshift parameters: {[p['name'] + '=' + p['value'] for p in parameters_list]}")

            if REDSHIFT_CLUSTER_ID and ('.' in REDSHIFT_CLUSTER_ID or ':' in REDSHIFT_CLUSTER_ID):
                cluster_name = REDSHIFT_CLUSTER_ID.split('.')[0]
                logger.info(f"Extracted cluster name from endpoint: {cluster_name}")
                
                redshift_client = boto3.client('redshift-data')
                response = redshift_client.execute_statement(
                    ClusterIdentifier=cluster_name,
                    Database=REDSHIFT_DB,
                    DbUser=REDSHIFT_USER,
                    Sql=insert_query,
                    Parameters=parameters_list
                )
            else:
                redshift_client = boto3.client('redshift-data')
                response = redshift_client.execute_statement(
                    ClusterIdentifier=REDSHIFT_CLUSTER_ID,
                    Database=REDSHIFT_DB,
                    DbUser=REDSHIFT_USER,
                    Sql=insert_query,
                    Parameters=parameters_list
                )
            logger.info(f"Successfully wrote KPIs to Redshift. QueryId: {response.get('Id', 'N/A')}")
            logger.info(f"Redshift write completed in {(datetime.utcnow() - redshift_start).total_seconds()} seconds")

            if len(crm_logs) > 0:
                logger.info(f"Storing {len(crm_logs)} CRM logs in Redshift (placeholder)")
                # Placeholder for raw data storage logic

            total_duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Lambda execution completed successfully in {total_duration} seconds")
            return {'statusCode': 200, 'body': 'KPIs and data written to Redshift'}
        except Exception as e:
            logger.error(f"Failed to write to Redshift: {str(e)}", exc_info=True)
            raise

    except Exception as e:
        logger.error(f"Error processing records: {str(e)}", exc_info=True)
        total_duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info(f"Lambda execution failed after {total_duration} seconds")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to process records',
                'details': str(e)
            })
        }