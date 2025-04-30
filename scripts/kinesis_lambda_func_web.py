import boto3
import base64
import json
import logging
import os
from botocore.exceptions import ClientError

# Setting up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialising DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get('DYNAMODB_TABLE')
table = dynamodb.Table(table_name)

# Lambda handler function to process Kinesis records and write to DynamoDB table
def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Decode base64 and parse JSON
            payload = base64.b64decode(record['kinesis']['data'])
            data = json.loads(payload)

            logger.info("Received record", extra={"record": data})

            # Write to DynamoDB
            response = table.put_item(Item=data)
            logger.info("Record written to DynamoDB", extra={
                "record": data,
                "dynamodb_response": response
            })

        except (json.JSONDecodeError, KeyError) as parse_err:
            logger.error("Failed to parse record", extra={
                "error": str(parse_err),
                "raw_record": record
            })

        except ClientError as client_err:
            logger.error("DynamoDB ClientError", extra={
                "error": str(client_err),
                "record": data
            })

        except Exception as e:
            logger.exception("Unexpected error occurred", extra={
                "error": str(e),
                "record": record
            })

    return {
        'statusCode': 200,
        'body': json.dumps('Kinesis records processed.')
    }
