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
    Returns the current timestamp in a formatted string.
    
    Returns:
        str: Current timestamp in YYYY-MM-DD-HH-MM-SS format
    """
    return datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')

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

def process_records(records, required_fields):
    """
    Process and validate records from Firehose.
    
    Args:
        records (list): List of records from Firehose
        required_fields (list): List of fields that must not be null
        
    Returns:
        tuple: (valid_records, dropped_count)
    """
    valid_records = []
    dropped_count = 0
    
    for record in records:
        try:
            # Decode and parse the record data
            payload = base64.b64decode(record['data'])
            json_data = json.loads(payload)
            
            # Validate required fields
            if validate_record(json_data, required_fields):
                valid_records.append(json_data)
            else:
                dropped_count += 1
                logger.warning(f"Dropped record due to null required fields: {json_data}")
        except json.JSONDecodeError:
            dropped_count += 1
            logger.error(f"Failed to decode JSON data: {payload}")
        except Exception as e:
            dropped_count += 1
            logger.error(f"Error processing record: {str(e)}")
    
    logger.info(f"Processed {len(records)} records: {len(valid_records)} valid, {dropped_count} dropped")
    return valid_records, dropped_count

def convert_to_parquet(records):
    """
    Convert JSON records to Parquet format.
    
    Args:
        records (list): List of JSON records
        
    Returns:
        bytes: Parquet file content as bytes
    """
    if not records:
        return None
    
    try:
        # Convert to pandas DataFrame
        df = pd.DataFrame(records)
        
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
    AWS Lambda handler for Kinesis Firehose delivery stream.
    Processes customer interaction data, converts from JSON to Parquet,
    and filters out records with null values in required fields.
    
    Args:
        event (dict): Lambda event data from Kinesis Firehose
        context (object): Lambda context object
        
    Returns:
        dict: Response to Kinesis Firehose with processing results
    """
    logger.info(f"Received event with {len(event['records'])} records")
    
    try:
        # Get destination bucket from environment variable
        destination_bucket = os.environ.get('DESTINATION_BUCKET')
        if not destination_bucket:
            logger.error("DESTINATION_BUCKET environment variable not set")
            raise ValueError("DESTINATION_BUCKET environment variable not set")
        
        # Define prefix for S3 destination
        prefix = os.environ.get('DESTINATION_PREFIX', 'customer-interactions/bronze/')
        
        # Define required fields based on schema (non-nullable attributes)
        required_fields = ['customer_id', 'interaction_type', 'timestamp']
        
        # Process records
        valid_records, dropped_count = process_records(event['records'], required_fields)
        
        # Prepare response for Firehose
        processed_records = []
        
        if valid_records:
            # Convert to Parquet
            parquet_data = convert_to_parquet(valid_records)
            
            if parquet_data:
                # Generate unique key with timestamp and partition by date
                timestamp = get_current_timestamp()
                date_partition = datetime.utcnow().strftime('%Y/%m/%d')
                key = f"{prefix}{date_partition}/interactions-{timestamp}.parquet"
                
                # Upload to S3
                success = upload_to_s3(parquet_data, destination_bucket, key)
                
                if not success:
                    logger.error("Failed to upload Parquet data to S3")
                    # Return all records as ProcessingFailed
                    for record in event['records']:
                        processed_records.append({
                            'recordId': record['recordId'],
                            'result': 'ProcessingFailed',
                            'data': record['data']
                        })
                    return {'records': processed_records}
        
        # Mark all records as processed
        for record in event['records']:
            processed_records.append({
                'recordId': record['recordId'],
                'result': 'Dropped' if dropped_count > 0 else 'Ok',
                'data': record['data']
            })
        
        logger.info(f"Successfully processed batch: {len(valid_records)} valid records, {dropped_count} dropped")
        return {'records': processed_records}
    
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        # Mark all records as processing failed
        processed_records = []
        for record in event['records']:
            processed_records.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']
            })
        return {'records': processed_records}