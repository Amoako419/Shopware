import json
import logging
import os
import boto3
import pandas as pd
from datetime import datetime
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

def get_current_date():
    """Return today's date in YYYY-MM-DD format."""
    return datetime.utcnow().strftime('%Y-%m-%d')

def get_unique_parquet_filename(bucket, prefix):
    """
    Generate a unique Parquet filename by counting existing files in the destination path.
    Args:
        bucket (str): Destination S3 bucket.
        prefix (str): Destination prefix (e.g., 'bronze-inventory/YYYY-MM-DD/').
    Returns:
        str: Unique filename (e.g., 'data1.parquet', 'data2.parquet').
    """
    try:
        # List objects in the destination prefix
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        # Count existing Parquet files
        parquet_count = 0
        if 'Contents' in response:
            parquet_count = sum(1 for obj in response['Contents'] if obj['Key'].endswith('.parquet'))
        # Generate the next filename (e.g., data1.parquet for count=0)
        return f"data{parquet_count + 1}.parquet"
    except ClientError as e:
        logger.error(f"Failed to list objects in s3://{bucket}/{prefix}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error generating unique filename: {str(e)}")
        return None

def parse_s3_event(s3_event):
    """
    Parse S3 event from SQS message to extract bucket and key.
    Args:
        s3_event (dict): S3 event data from SQS message.
    Returns:
        tuple: (bucket_name, object_key) or (None, None) if parsing fails.
    """
    try:
        bucket = s3_event['s3']['bucket']['name']
        key = unquote_plus(s3_event['s3']['object']['key'])
        logger.info(f"Parsed S3 event: bucket={bucket}, key={key}")
        return bucket, key
    except KeyError as e:
        logger.error(f"Failed to parse S3 event: {str(e)}")
        return None, None

def read_json_from_s3(bucket, key):
    """
    Read a JSON file from S3 into a pandas DataFrame.
    Args:
        bucket (str): S3 bucket name.
        key (str): S3 object key.
    Returns:
        pandas.DataFrame or None if reading fails.
    """
    try:
        logger.info(f"Reading JSON from s3://{bucket}/{key}")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_json(obj['Body'])
        logger.info(f"Successfully read JSON: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except ClientError as e:
        logger.error(f"Failed to read JSON from S3: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error reading JSON: {str(e)}")
        return None

def write_parquet_to_s3(df, bucket, destination_key):
    """
    Write a DataFrame to S3 as a Parquet file.
    Args:
        df (pandas.DataFrame): Data to write.
        bucket (str): Destination S3 bucket.
        destination_key (str): Destination S3 key.
    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        logger.info(f"Writing Parquet to s3://{bucket}/{destination_key}")
        # Convert DataFrame to Parquet in memory
        parquet_buffer = df.to_parquet(engine='pyarrow', index=False)
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=destination_key,
            Body=parquet_buffer,
            ContentType='application/x-parquet'
        )
        logger.info(f"Successfully wrote Parquet to s3://{bucket}/{destination_key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to write Parquet to S3: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error writing Parquet: {str(e)}")
        return False

def process_s3_event(s3_event, source_bucket_name, destination_bucket_name, source_prefix, destination_prefix):
    """
    Process an S3 event by reading JSON, converting to Parquet, and saving to destination bucket.
    Args:
        s3_event (dict): S3 event data.
        source_bucket_name (str): Source S3 bucket name from environment variable.
        destination_bucket_name (str): Destination S3 bucket name from environment variable.
        source_prefix (str): Expected source prefix (e.g., 'inventory/inventory_landing_data/').
        destination_prefix (str): Destination prefix (e.g., 'bronze-inventory/').
    Returns:
        bool: True if successful, False otherwise.
    """
    bucket, key = parse_s3_event(s3_event)
    if not bucket or not key:
        logger.error("Invalid S3 event data")
        return False

    # Verify bucket matches source bucket
    if bucket != source_bucket_name:
        logger.warning(f"Skipping file from bucket {bucket}: does not match expected source bucket {source_bucket_name}")
        return False

    # Verify source path
    if not key.startswith(source_prefix):
        logger.warning(f"Skipping file {key}: does not match source prefix {source_prefix}")
        return False

    # Verify file ends with .json
    if not key.lower().endswith('.json'):
        logger.warning(f"Skipping file {key}: not a JSON file")
        return False

    # Read JSON from source bucket
    df = read_json_from_s3(bucket, key)
    if df is None:
        logger.error("Failed to read JSON data")
        return False

    # Generate destination prefix with today's date
    today = get_current_date()
    destination_path = f"{destination_prefix}{today}/"

    # Get unique filename (e.g., data1.parquet)
    filename = get_unique_parquet_filename(destination_bucket_name, destination_path)
    if not filename:
        logger.error("Failed to generate unique Parquet filename")
        return False

    # Construct destination key
    destination_key = f"{destination_path}{filename}"

    # Write Parquet to destination bucket
    success = write_parquet_to_s3(df, destination_bucket_name, destination_key)
    if not success:
        logger.error("Failed to write Parquet data")
        return False

    return True

def lambda_handler(event, context):
    """
    AWS Lambda handler to process SQS messages containing S3 events.
    Args:
        event (dict): Lambda event data (SQS messages).
        context (object): Lambda context object.
    Returns:
        dict: Lambda response.
    """
    # Fetch bucket names from environment variables
    try:
        source_bucket_name = os.environ['SOURCE_BUCKET_NAME']
        destination_bucket_name = os.environ['DESTINATION_BUCKET_NAME']
        logger.info(f"Using source bucket: {source_bucket_name}, destination bucket: {destination_bucket_name}")
    except KeyError as e:
        logger.error(f"Environment variable {str(e)} not set")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Environment variable {str(e)} missing"})
        }

    logger.info(f"Received event with {len(event['Records'])} records")
    
    source_prefix = "inventory/"
    destination_prefix = "bronze-inventory/"
    processed_records = 0
    failed_records = 0

    for record in event['Records']:
        try:
            # Parse SQS message
            message_body = json.loads(record['body'])
            logger.info(f"Processing SQS message: {record['messageId']}")

            # Skip test events (for local testing)            
            # Ignore s3:TestEvent messages
            if message_body.get('Event') == 's3:TestEvent':
                logger.warning("Skipping s3:TestEvent message (test event)")
                continue

            # S3 events may be wrapped in an SNS or direct S3 notification
            if 'Records' in message_body:
                # S3 event notification
                for s3_record in message_body['Records']:
                    success = process_s3_event(
                        s3_record, 
                        source_bucket_name, 
                        destination_bucket_name, 
                        source_prefix, 
                        destination_prefix
                    )
                    if success:
                        processed_records += 1
                    else:
                        failed_records += 1
            else:
                logger.warning(f"Unexpected message format: {json.dumps(message_body)}")
                failed_records += 1

            # Delete message from SQS to prevent reprocessing
            sqs_client.delete_message(
                QueueUrl=record['eventSourceARN'].split(':')[5],
                ReceiptHandle=record['receiptHandle']
            )
            logger.info(f"Deleted SQS message: {record['messageId']}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse SQS message body: {str(e)}")
            failed_records += 1
        except ClientError as e:
            logger.error(f"AWS API error processing record: {str(e)}")
            failed_records += 1
        except Exception as e:
            logger.error(f"Unexpected error processing record: {str(e)}")
            failed_records += 1

    logger.info(f"Processing complete: {processed_records} succeeded, {failed_records} failed")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed_records,
            'failed': failed_records
        })
    }