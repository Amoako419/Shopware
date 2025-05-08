# Lambda Functions

This directory contains all AWS Lambda functions used in the Shopware Data Pipeline for real-time data processing and KPI calculations.

## Functions Overview

- `crm_kinesis_lambda_func.py`: Processes CRM interaction data from Kinesis streams
- `customer_interaction_firehose.py`: Handles customer interaction data delivery to S3 via Kinesis Firehose
- `customer_support_kpis.py`: Calculates customer support KPIs from processed data
- `inventory_lambda.py`: Processes inventory updates and calculates stock-related metrics
- `marketing-kpis.py`: Computes marketing KPIs from web traffic and customer data
- `pos_landing_to_bronze.py`: Transforms POS data from landing to bronze layer
- `web_kinesis_lambda_func.py`: Processes real-time web traffic data from Kinesis streams

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables as specified in each function's documentation

## Deployment

Functions are deployed via Terraform. See the main project README for deployment instructions.

## Testing

Each function has associated unit tests in the `tests` directory. Run tests using:
```bash
python -m pytest
```

## Function Details

### CRM and Web Traffic Processing
- Handles real-time data streams
- Calculates KPIs in near real-time
- Stores results in Redshift and S3

### Batch Data Processing
- Transforms POS and inventory data
- Implements data quality checks
- Manages data partitioning in S3

## Security

- Uses AWS Secrets Manager for credentials
- Implements least privilege IAM roles
- Encrypts data in transit and at rest

## Monitoring

All functions are monitored via CloudWatch:
- Metrics for invocations and errors
- Custom metrics for business KPIs
- Log retention set to 30 days