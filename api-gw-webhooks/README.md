# API Gateway Webhooks

This directory contains the infrastructure and code for handling real-time data ingestion via API Gateway webhooks.

## Directory Structure

- `crm-logs-infra/`: Infrastructure for CRM interaction data ingestion
- `web-logs-infra/`: Infrastructure for web traffic data ingestion

Each subdirectory contains:
- `connector/`: Python application for processing incoming data
- `proxy/`: API Gateway proxy integration code
- `scripts/`: Deployment and build scripts
- `terraform/`: Infrastructure as Code for AWS resources

## Setup and Deployment

### Prerequisites
- Docker installed
- AWS CLI configured
- Terraform installed
- Valid AWS credentials

### Deployment Steps

1. Build and push Docker images:
```bash
cd crm-logs-infra/scripts
./build_push_ecr.sh

cd ../../web-logs-infra/scripts
./build_push_ecr.sh
```

2. Deploy infrastructure:
```bash
cd ../terraform
terraform init
terraform apply
```

## Architecture

- API Gateway receives webhook requests
- Requests are validated and transformed
- Data is sent to Kinesis Data Streams
- Lambda functions process the streams
- Processed data stored in S3 and Redshift

## Security

- API keys for webhook authentication
- VPC endpoints for internal AWS services
- Encryption in transit and at rest
- IAM roles following least privilege principle

## Monitoring

- CloudWatch logs for API Gateway
- CloudWatch metrics for request tracking
- Lambda function monitoring
- Custom metrics for data validation