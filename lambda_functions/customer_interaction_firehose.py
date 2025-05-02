import json
import logging
import base64
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        list: List of processed records with their processing status
    """
    processed_records = []
    valid_count = 0
    dropped_count = 0
    
    for record in records:
        record_id = record['recordId']
        try:
            # Decode and parse the record data
            payload = base64.b64decode(record['data'])
            json_data = json.loads(payload)
            
            # Validate required fields
            if validate_record(json_data, required_fields):
                # Record is valid, keep it as is
                processed_records.append({
                    'recordId': record_id,
                    'result': 'Ok',
                    'data': record['data']  # Return original data for Firehose to process
                })
                valid_count += 1
            else:
                # Drop record with null required fields
                logger.warning(f"Dropped record due to null required fields: {json_data}")
                processed_records.append({
                    'recordId': record_id,
                    'result': 'Dropped',
                    'data': record['data']  # Return original data but mark as Dropped
                })
                dropped_count += 1
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON data for record {record_id}")
            processed_records.append({
                'recordId': record_id,
                'result': 'ProcessingFailed',
                'data': record['data']
            })
            dropped_count += 1
        except Exception as e:
            logger.error(f"Error processing record {record_id}: {str(e)}")
            processed_records.append({
                'recordId': record_id,
                'result': 'ProcessingFailed',
                'data': record['data']
            })
            dropped_count += 1
    
    logger.info(f"Processed {len(records)} records: {valid_count} valid, {dropped_count} dropped")
    return processed_records

def lambda_handler(event, context):
    """
    AWS Lambda handler for Kinesis Firehose delivery stream.
    Validates customer interaction data and filters out records with null values in required fields.
    Firehose will handle the conversion to Parquet and delivery to S3.
    
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
        
        # Process and validate records
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