import json
import logging
import base64
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime
import boto3
import os
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')

def get_current_timestamp():
    """
    Returns the current timestamp in a formatted string with millisecond precision.
    
    Returns:
        str: Current timestamp in YYYY-MM-DD-HH-MM-SS-mmm format
    """
    return datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S-%f')[:-3]

def validate_record(record, required_fields):
    """
    Validates that all required fields in a record are not null.
    
    Args:
        record (dict): The record to validate
        required_fields (list): List of field names that must not be null
        
    Returns:
        bool: True if all required fields have values, False otherwise
    """
    for field in required_fields:
        if field not in record or record[field] is None:
            return False
    return True

def process_record(record_data, required_fields):
    """
    Process and validate a single record.
    
    Args:
        record_data (dict): The record data to process
        required_fields (list): List of fields that must not be null
        
    Returns:
        tuple: (processed_record, is_valid)
    """
    try:
        # Validate required fields
        if validate_record(record_data, required_fields):
            return record_data, True
        else:
            logger.warning(f"Dropped record due to null required fields: {record_data}")
            return None, False
    except Exception as e:
        logger.error(f"Error processing record: {str(e)}")
        return None, False

def convert_to_parquet(record):
    """
    Convert a single JSON record to Parquet format.
    
    Args:
        record (dict): JSON record
        
    Returns:
        bytes: Parquet file content as bytes
    """
    if not record:
        return None
    
    try:
        # Convert to pandas DataFrame (single row)
        df = pd.DataFrame([record])
        
        # Convert to parquet bytes
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)
        return parquet_buffer.getvalue()
    except Exception as e:
        logger.error(f"Error converting to Parquet: {str(e)}")
        return None

def upload_to_s3(data, bucket, key):
    """
    Upload data to S3.
    
    Args:
        data (bytes): Data to upload
        bucket (str): S3 bucket name
        key (str): S3 object key
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType='application/x-parquet'
        )
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error uploading to S3: {str(e)}")
        return False

def lambda_handler(event, context):
    """
    AWS Lambda handler for Kinesis Data Streams.
    Processes customer interaction data in real-time, converts from JSON to Parquet,
    and filters out records with null values in required fields.
    
    Args:
        event (dict): Lambda event data from Kinesis Data Stream
        context (object): Lambda context object
        
    Returns:
        dict: Processing summary
    """
    try:
        # Get destination bucket from environment variable
        destination_bucket = os.environ.get('DESTINATION_BUCKET')
        if not destination_bucket:
            logger.error("DESTINATION_BUCKET environment variable not set")
            raise ValueError("DESTINATION_BUCKET environment variable not set")
        
        # Define prefix for S3 destination
        prefix = os.environ.get('DESTINATION_PREFIX', 'customer-interactions/real-time/')
        
        # Define required fields based on schema (non-nullable attributes)
        required_fields = ['customer_id', 'interaction_type', 'timestamp']
        
        # Process statistics
        processed_count = 0
        valid_count = 0
        dropped_count = 0
        
        # Process each record individually for real-time processing
        for record in event['Records']:
            processed_count += 1
            
            try:
                # Decode and parse the record data
                payload = base64.b64decode(record['kinesis']['data'])
                record_data = json.loads(payload)
                
                # Process the record
                processed_record, is_valid = process_record(record_data, required_fields)
                
                if is_valid:
                    valid_count += 1
                    
                    # Convert to Parquet
                    parquet_data = convert_to_parquet(processed_record)
                    
                    if parquet_data:
                        # Generate unique key with timestamp and partition by date/hour for real-time data
                        timestamp = get_current_timestamp()
                        date_hour_partition = datetime.utcnow().strftime('%Y/%m/%d/%H')
                        
                        # Include customer_id in the path for potential partitioning benefit
                        customer_id = str(processed_record['customer_id'])
                        
                        # Create a unique key for each record
                        key = f"{prefix}{date_hour_partition}/customer_id={customer_id}/interaction-{timestamp}.parquet"
                        
                        # Upload to S3 immediately
                        upload_to_s3(parquet_data, destination_bucket, key)
                else:
                    dropped_count += 1
                    
            except json.JSONDecodeError as e:
                dropped_count += 1
                logger.error(f"Failed to decode JSON data: {str(e)}")
            except Exception as e:
                dropped_count += 1
                logger.error(f"Error processing Kinesis record: {str(e)}")
        
        logger.info(f"Real-time processing complete: {processed_count} records processed, {valid_count} valid, {dropped_count} dropped")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed': processed_count,
                'valid': valid_count,
                'dropped': dropped_count
            })
        }
    
    except Exception as e:
        logger.error(f"Unhandled exception in Lambda handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }