import json, os, uuid, datetime, logging, boto3, tempfile
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

TARGET_BUCKET = os.environ.get('S3_BRONZE_BUCKET')
TARGET_PREFIX = os.environ.get('S3_BRONZE_PREFIX')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Parse the S3 event from the SQS message body
            message_body = json.loads(record['body'])
            detail = message_body.get('detail', {})
            
            source_bucket = detail['bucket']['name']
            source_key = detail['object']['key']
            logger.info(f"Processing file: s3://{source_bucket}/{source_key}")

            # Download CSV from S3
            with tempfile.NamedTemporaryFile() as temp_input:
                s3.download_file(source_bucket, source_key, temp_input.name)

                # Read the CSV into a DataFrame
                df = pd.read_csv(temp_input.name) 

                # Generate unique Parquet filename
                base_filename = os.path.splitext(os.path.basename(source_key))[0]
                timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                unique_id = str(uuid.uuid4())
                bronze_key = f"{TARGET_PREFIX}{timestamp}_{unique_id}_{base_filename}.parquet"

                # Save DataFrame as Parquet to /tmp
                with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_output:
                    df.to_parquet(temp_output.name, index=False)

                    # Upload Parquet to bronze bucket
                    s3.upload_file(temp_output.name, TARGET_BUCKET, bronze_key)

            logger.info(f"Parquet saved: s3://{TARGET_BUCKET}/{bronze_key}")

            # Optionally delete the original CSV
            s3.delete_object(Bucket=source_bucket, Key=source_key)
            logger.info(f"Deleted original: s3://{source_bucket}/{source_key}")

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            raise e
