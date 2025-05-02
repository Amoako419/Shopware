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

def process_records(records, required_fields):
    """
    Process and validate records from Firehose.
    For near real-time processing, we process each record individually.
    
    Args:
        records (list): List of records from Firehose
        required_fields (list): List of fields that must not be null
        
    Returns:
        list: List of processed records with their processing status
    """
    processed_records = []
    
    for record in records:
        record_id = record['recordId']
        try:
            # Decode and parse the record data
            payload = base64.b64decode(record['data'])
            json_data = json.loads(payload)
            
            # Validate required fields
            if validate_record(json_data, required_fields):
                # Convert single record to Parquet
                parquet_data = convert_to_parquet(json_data)
                
                if parquet_data:
                    # Get destination bucket from environment variable
                    destination_bucket = os.environ.get('DESTINATION_BUCKET')
                    if not destination_bucket:
                        logger.error("DESTINATION_BUCKET environment variable not set")
                        processed_records.append({
                            'recordId': record_id,
                            'result': 'ProcessingFailed',
                            'data': record['data']
                        })
                        continue
                    
                    # Define prefix for S3 destination
                    prefix = os.environ.get('DESTINATION_PREFIX', 'customer-interactions/bronze/')
                    
                    # Generate unique key with timestamp and partition by date/hour/minute for near real-time data
                    timestamp = get_current_timestamp()
                    date_time_partition = datetime.utcnow().strftime('%Y/%m/%d/%H/%M')
                    
                    # Include customer_id in the path for partitioning
                    customer_id = str(json_data['customer_id'])
                    
                    # Create a unique key for this record
                    key = f"{prefix}{date_time_partition}/customer_id={customer_id}/interaction-{timestamp}.parquet"
                    
                    # Upload to S3
                    success = upload_to_s3(parquet_data, destination_bucket, key)
                    
                    if success:
                        # Mark as processed successfully
                        processed_records.append({
                            'recordId': record_id,
                            'result': 'Ok',
                            'data': record['data']
                        })
                    else:
                        # Mark as processing failed
                        processed_records.append({
                            'recordId': record_id,
                            'result': 'ProcessingFailed',
                            'data': record['data']
                        })
                else:
                    # Mark as processing failed
                    processed_records.append({
                        'recordId': record_id,
                        'result': 'ProcessingFailed',
                        'data': record['data']
                    })
            else:
                # Drop record with null required fields
                logger.warning(f"Dropped record due to null required fields: {json_data}")
                processed_records.append({
                    'recordId': record_id,
                    'result': 'Dropped',
                    'data': record['data']
                })
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON data for record {record_id}")
            processed_records.append({
                'recordId': record_id,
                'result': 'ProcessingFailed',
                'data': record['data']
            })
        except Exception as e:
            logger.error(f"Error processing record {record_id}: {str(e)}")
            processed_records.append({
                'recordId': record_id,
                'result': 'ProcessingFailed',
                'data': record['data']
            })
    
    return processed_records

def convert_to_parquet(record):
    """
    Convert a single JSON record to Parquet format.
    
    Args:
        record (dict): JSON record
        
    Returns:
        bytes: Parquet file content as bytes
    """
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
    AWS Lambda handler for Kinesis Firehose delivery stream.
    Processes customer interaction data in near real-time, converts from JSON to Parquet,
    and filters out records with null values in required fields.
    
    This implementation processes each record individually for near real-time availability.
    
    Args:
        event (dict): Lambda event data from Kinesis Firehose
        context (object): Lambda context object
        
    Returns:
        dict: Response to Kinesis Firehose with processing results
    """
    logger.info(f"Received event with {len(event['records'])} records")
    
    try:
        # Define required fields based on schema (non-nullable attributes)
        required_fields = ['customer_id', 'interaction_type', 'timestamp']
        
        # Process each record individually for near real-time processing
        processed_records = process_records(event['records'], required_fields)
        
        # Return processing results to Firehose
        logger.info(f"Successfully processed {len(processed_records)} records")
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