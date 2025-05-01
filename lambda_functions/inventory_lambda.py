
import json
import logging
import os
import boto3
import pandas as pd
import io
from datetime import datetime
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 

# AWS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

def get_current_date():
    return datetime.utcnow().strftime('%Y-%m-%d')

def parse_s3_event(s3_event):
    try:
        bucket = s3_event['s3']['bucket']['name']
        key = unquote_plus(s3_event['s3']['object']['key'])
        logger.info(f"Parsed S3 event: bucket={bucket}, key={key}")
        return bucket, key
    except KeyError as e:
        logger.error(f"Error parsing event: {str(e)}")
        return None, None

def read_csv_from_s3(bucket, key):
    try:
        logger.info(f"Reading CSV from s3://{bucket}/{key}")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj['Body'])
        logger.info(f"Read CSV: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV from S3: {str(e)}")
        return None

def write_parquet_to_s3(df, bucket, destination_key):
    try:
        logger.info(f"Writing Parquet to s3://{bucket}/{destination_key}")
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Key=destination_key, Body=buffer.getvalue(),
                             ContentType='application/x-parquet')
        logger.info("Successfully uploaded Parquet")
        return True
    except Exception as e:
        logger.error(f"Error writing Parquet to S3: {str(e)}")
        return False

def process_s3_event(s3_event, bucket_name, source_prefix, destination_prefix):
    bucket, key = parse_s3_event(s3_event)
    if not bucket or not key:
        return False
    if bucket != bucket_name:
        logger.warning(f"Skipping file from different bucket: {bucket}")
        return False
    if not key.startswith(source_prefix) or not key.lower().endswith('.csv'):
        logger.warning(f"Invalid file path or format: {key}")
        return False

    df = read_csv_from_s3(bucket, key)
    if df is None:
        return False

    today = get_current_date()
    dest_key = f"{destination_prefix}{today}/data.parquet"
    return write_parquet_to_s3(df, bucket, dest_key)

def lambda_handler(event, context):
    try:
        bucket_name = os.environ['S3_BUCKET_NAME']
    except KeyError:
        logger.error("Missing BUCKET_NAME env variable")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'BUCKET_NAME env variable not set'})
        }

    logger.info(f"Received {len(event['Records'])} SQS records")
    source_prefix = "inventory/inventory_landing_data/"
    destination_prefix = "bronze-inventory/"
    success_count, fail_count = 0, 0

    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            logger.info(f"Processing record: {record['messageId']}")
            if 'Records' in body:
                for s3_event in body['Records']:
                    if process_s3_event(s3_event, bucket_name, source_prefix, destination_prefix):
                        success_count += 1
                    else:
                        fail_count += 1
            else:
                logger.warning(f"Invalid message format: {body}")
                fail_count += 1

            # Delete from queue
            sqs_client.delete_message(
                QueueUrl=record['eventSourceARN'].split(':')[5],
                ReceiptHandle=record['receiptHandle']
            )
            logger.info(f"Deleted message {record['messageId']}")
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            fail_count += 1

    logger.info(f"Done: {success_count} succeeded, {fail_count} failed")
    return {
        'statusCode': 200,
        'body': json.dumps({'processed': success_count, 'failed': fail_count})
    }
