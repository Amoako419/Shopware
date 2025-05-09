import base64
import json
import logging
import os
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

# Set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create DynamoDB low-level client
dynamodb = boto3.client("dynamodb")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE")


def convert_to_dynamodb_format(data):
    """
    Convert Python dict to DynamoDB low-level format.
    Handles strings, numbers (including Decimal), and nested dicts/lists.
    """

    def serialize_value(val):
        if isinstance(val, str):
            return {"S": val}
        elif isinstance(val, (int, float, Decimal)):
            return {"N": str(val)}
        elif isinstance(val, bool):
            return {"BOOL": val}
        elif isinstance(val, list):
            return {"L": [serialize_value(v) for v in val]}
        elif isinstance(val, dict):
            return {"M": {k: serialize_value(v) for k, v in val.items()}}
        elif val is None:
            return {"NULL": True}
        else:
            raise TypeError(f"Unsupported type: {type(val)}")

    return {k: serialize_value(v) for k, v in data.items()}


def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            # Decode base64 and parse JSON with float support
            payload = base64.b64decode(record["kinesis"]["data"])
            data = json.loads(payload, parse_float=Decimal)

            logger.info("Decoded data", extra={"record": data})

            # Convert data to DynamoDB format
            item = convert_to_dynamodb_format(data)

            # Put item into DynamoDB
            response = dynamodb.put_item(TableName=TABLE_NAME, Item=item)
            logger.info("PutItem succeeded", extra={"response": response})

        except ClientError as ce:
            logger.error(
                "DynamoDB ClientError", extra={"error_message": str(ce), "item": data}
            )
        except Exception as e:
            logger.exception("Unexpected error occurred", extra={"record": record})

    return {"statusCode": 200, "body": json.dumps("Records processed.")}
