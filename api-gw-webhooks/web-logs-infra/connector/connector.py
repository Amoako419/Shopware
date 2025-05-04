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

# Environment variables
SOURCE_URL = "http://18.203.232.58:8000/api/web-traffic/"
STREAM_NAME = "shopware-web-logs-firehose"
REGION = "eu-west-1"
POLL_INTERVAL =  10
BATCH_SIZE = 500           # Max records per batch

# AWS Firehose client
firehose = boto3.client("firehose", region_name=REGION)

def fetch_data():
    try:
        response = requests.get(SOURCE_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        return []

def send_to_firehose(records):
    if not records:
        return

    try:
        entries = [{
            "Data": json.dumps(record) + "\n"
        } for record in records]

        response = firehose.put_record_batch(
            DeliveryStreamName=STREAM_NAME,
            Records=entries
        )

        failed = response['FailedPutCount']
        if failed:
            logger.warning(f"{failed} records failed to send.")
        else:
            logger.info(f"Successfully sent {len(records)} records.")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Firehose error: {e}")

def main():
    logger.info("Starting connector loop.")
    buffer = []

    while True:
        new_data = fetch_data()
        buffer.extend(new_data)

        if len(buffer) >= BATCH_SIZE:
            send_to_firehose(buffer[:BATCH_SIZE])
            buffer = buffer[BATCH_SIZE:]

        elif new_data:
            # Send partial buffer if new data came in
            send_to_firehose(buffer)
            buffer = []

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
