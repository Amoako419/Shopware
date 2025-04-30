import json, os, uuid, datetime, logging, boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

# Target bucket and prefix
TARGET_BUCKET = os.environ.get('S3_BRONZE_BUCKET')
TARGET_PREFIX = os.environ.get('S3_BRONZE_PREFIX')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Step 1: Parse the S3 event from the SQS message body
            message_body = json.loads(record['body'])
            detail = message_body.get('detail', {})
            
            source_bucket = detail['bucket']['name']
            source_key = detail['object']['key']

            logger.info(f"Processing file from: s3://{source_bucket}/{source_key}")

            # Step 2: Generate a unique filename for the bronze layer
            base_filename = os.path.basename(source_key)
            timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            unique_id = str(uuid.uuid4())
            bronze_key = f"{TARGET_PREFIX}{timestamp}_{unique_id}_{base_filename}"

            # Step 3: Copy object to bronze bucket
            s3.copy_object(
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Bucket=TARGET_BUCKET,
                Key=bronze_key
            )
            logger.info(f"Copied to: s3://{TARGET_BUCKET}/{bronze_key}")

            # Step 4: Optionally delete original object
            s3.delete_object(Bucket=source_bucket, Key=source_key)
            logger.info(f"Deleted original: s3://{source_bucket}/{source_key}")

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            raise e  # So the message returns to SQS for retry
