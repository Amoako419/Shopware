import os
import time
import json
import logging
import base64
import requests
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Variables
SOURCE_URL = "http://18.203.232.58:8000/api/web-traffic/"
STREAM_NAME = "web-logs-stream"  # Changed from firehose to a streams name
REGION = "eu-west-1"
POLL_INTERVAL = 5
BATCH_SIZE = 200  

# AWS Kinesis client
kinesis = boto3.client("kinesis", region_name=REGION)

def fetch_data():
    try:
        response = requests.get(SOURCE_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        return []

def send_to_kinesis(records):
    if not records:
        return
    
    try:
        # Kinesis Data Streams requires a PartitionKey for each record
        entries = [{
            "Data": json.dumps(record).encode('utf-8'),
            "PartitionKey": str(record.get('id', hash(json.dumps(record))))  # Use id as partition key if available
        } for record in records]
        
        # Kinesis PutRecords API call
        response = kinesis.put_records(
            Records=entries,
            StreamName=STREAM_NAME
        )
        
        failed = response.get('FailedRecordCount', 0)
        if failed:
            # Log details about failed records
            failed_records = [
                (i, response['Records'][i].get('ErrorCode'), response['Records'][i].get('ErrorMessage'))
                for i in range(len(response['Records']))
                if 'ErrorCode' in response['Records'][i]
            ]
            logger.warning(f"{failed} records failed to send: {failed_records}")
        else:
            logger.info(f"Successfully sent {len(records)} records to Kinesis Data Stream.")
            
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Kinesis error: {e}")

def main():
    logger.info(f"Starting connector loop. Sending to Kinesis Data Stream: {STREAM_NAME}")
    buffer = []
    
    while True:
        try:
            new_data = fetch_data()
            logger.info(f"Fetched {len(new_data)} new records")
            
            buffer.extend(new_data)
            
            if len(buffer) >= BATCH_SIZE:
                # Send full batches
                send_to_kinesis(buffer[:BATCH_SIZE])
                buffer = buffer[BATCH_SIZE:]
            elif new_data:
                # Send partial buffer if new data came in
                send_to_kinesis(buffer)
                buffer = []
                
            time.sleep(POLL_INTERVAL)
            
        except Exception as e:
            logger.error(f"Error in main processing loop: {e}")
            time.sleep(POLL_INTERVAL)  # Wait before retry

if __name__ == "__main__":  # Fixed syntax error here
    main()