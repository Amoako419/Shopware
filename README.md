# Shopware Data Pipeline

## Overview

The Shopware Data Pipeline is a comprehensive data engineering solution designed to collect, process, and analyze web traffic and customer interaction data from Shopware e-commerce platforms. The pipeline follows the Medallion Architecture (Bronze, Silver, Gold layers) to transform raw data into valuable business insights.

## Architecture

The architecture follows a modern data lakehouse approach with three distinct layers:

1. **Bronze Layer (Raw Data)**: Initial data ingestion from source systems
2. **Silver Layer (Processed Data)**: Cleaned, validated, and transformed data
3. **Gold Layer (Business Insights)**: Aggregated data ready for analytics and reporting

![Shopware Data Pipeline Architecture](architecture-diagram-medallion.jpg)

## Data Sources

The pipeline ingests data from two primary sources:
- **Web Traffic API**: Captures user behavior, page views, and session data
- **CRM Interaction API**: Captures customer interactions and loyalty program activities

## Components

### Data Ingestion

1. **ECS Fargate Connectors**:
   - Python applications running in Docker containers
   - Poll source APIs at regular intervals (every 5 seconds)
   - Buffer and batch data before sending to Kinesis

2. **API Gateway Webhooks**:
   - HTTP endpoints for external systems to push data
   - Lambda functions process incoming requests
   - Data is forwarded to Kinesis Data Streams

### Data Storage & Processing

1. **Kinesis Data Streams**:
   - Real-time data streaming for both web and CRM data
   - Configured with proper sharding for scalability

2. **Kinesis Data Analytics**:
   - Real-time processing of streaming data
   - Detects patterns and anomalies in the data flow

3. **Kinesis Data Firehose**:
   - Delivers data to S3 for long-term storage
   - Configures data format and partitioning

4. **S3 Data Lake**:
   - Stores data in the medallion architecture pattern
   - Bronze bucket: Raw data
   - Silver bucket: Processed data
   - Gold bucket: Analytics-ready data

5. **AWS Glue**:
   - ETL jobs for data transformation between layers
   - Data Catalog for metadata management
   - Crawlers to discover and catalog data

6. **AWS Lambda**:
   - Event-driven processing
   - Connects various components of the pipeline
   - Handles API Gateway requests

### Analytics & Visualization

1. **Amazon Athena**:
   - SQL queries against data in S3
   - Ad-hoc analysis capabilities

2. **Amazon SageMaker**:
   - Machine learning models for predictive analytics
   - Customer segmentation and behavior analysis

3. **Business Intelligence Tools**:
   - Connect to processed data for dashboards and reports

## Key Performance Indicators (KPIs)

The pipeline calculates several business-critical KPIs:

1. **Marketing KPIs**:
   - Customer Engagement Score
   - Session Duration & Bounce Rate
   - Loyalty Activity Rate

2. **Sales KPIs**:
   - Conversion Rates
   - Average Order Value
   - Stock Availability

3. **Operations KPIs**:
   - Inventory Turnover
   - Restock Frequency
   - Stockout Alerts

## Setup Instructions

### Prerequisites

- AWS Account with appropriate permissions
- AWS CLI configured locally
- Docker installed locally
- Terraform installed locally
- Python 3.11 or higher

### Deployment Steps

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd shopware-data-pipeline
   ```

2. **Build and Push Docker Images**:
   ```bash
   # For CRM Logs Connector
   cd api-gw-webhooks/crm-logs-infra/scripts
   ./build_push_ecr.sh
   
   # For Web Logs Connector
   cd api-gw-webhooks/web-logs-infra/scripts
   ./build_push_ecr.sh
   ```

3. **Deploy Infrastructure with Terraform**:
   ```bash
   # For CRM Logs Infrastructure
   cd api-gw-webhooks/crm-logs-infra/terraform
   terraform init
   terraform apply
   
   # For Web Logs Infrastructure
   cd api-gw-webhooks/web-logs-infra/terraform
   terraform init
   terraform apply
   ```

4. **Deploy Glue Jobs**:
   - Upload Glue scripts to S3
   - Create and configure Glue jobs using the AWS Console or Terraform

5. **Verify Deployment**:
   - Check AWS Console to ensure all resources are created
   - Test webhook endpoints
   - Monitor CloudWatch logs for connector applications

### Configuration

Key configuration files:
- `api-gw-webhooks/crm-logs-infra/terraform/terraform.tfvars`: CRM pipeline configuration
- `api-gw-webhooks/web-logs-infra/terraform/terraform.tfvars`: Web traffic pipeline configuration
- `api-gw-webhooks/crm-logs-infra/connector/.env`: CRM connector environment variables
- `api-gw-webhooks/web-logs-infra/connector/.env`: Web connector environment variables

## Data Flow

### Bronze Layer (Data Ingestion)

1. **Data Collection**:
   - ECS Fargate connectors poll source APIs:
     - Web traffic: `http://18.203.232.58:8000/api/web-traffic/`
     - CRM data: `http://18.203.232.58:8000/api/customer-interaction/`
   - Data is sent to Kinesis Data Streams
   - External systems can push data via API Gateway webhooks

2. **Raw Storage**:
   - Kinesis Firehose delivers raw data to S3 bronze buckets
   - Data is partitioned by date

### Silver Layer (Data Processing)

1. **Data Cleaning & Validation**:
   - AWS Glue jobs process raw data from bronze layer
   - Data is validated, cleaned, and transformed
   - Schema enforcement and data quality checks
   - Results stored in S3 silver buckets

2. **Data Catalog**:
   - AWS Glue Crawlers catalog the processed data
   - Tables are created in the Glue Data Catalog

### Gold Layer (Analytics)

1. **KPI Calculation**:
   - Glue jobs calculate business KPIs:
     - `marketing-kpi.py`: Marketing metrics
     - `sales_glue_script.py`: Sales metrics
     - `operations_kpis.py`: Operations metrics
   - Results stored in S3 gold buckets

2. **Analytics Access**:
   - Amazon Athena for SQL queries
   - Notebooks for data science workflows
   - BI tools for dashboards

## Monitoring and Maintenance

1. **CloudWatch Monitoring**:
   - Logs from all components
   - Metrics for Kinesis, Lambda, and ECS
   - Alarms for critical thresholds

2. **Error Handling**:
   - Automatic retries in connectors
   - Dead-letter queues for failed processing
   - Error notifications via SNS

3. **Scaling**:
   - ECS services scale based on CPU/memory usage
   - Kinesis streams can be resharded for higher throughput
   - Glue jobs configured with appropriate DPUs

## Project Structure

```
shopware-data-pipeline/
├── api-gw-webhooks/
│   ├── crm-logs-infra/
│   │   ├── connector/
│   │   │   ├── connector.py       # Python script to poll CRM API
│   │   │   ├── Dockerfile         # Container definition
│   │   │   └── requirements.txt   # Python dependencies
│   │   ├── scripts/
│   │   │   └── build_push_ecr.sh  # Script to build and push Docker image
│   │   └── terraform/
│   │       ├── main.tf            # Infrastructure as code
│   │       └── terraform.tfvars   # Configuration values
│   └── web-logs-infra/
│       ├── connector/
│       │   ├── connector.py       # Python script to poll web traffic API
│       │   ├── Dockerfile         # Container definition
│       │   └── requirements.txt   # Python dependencies
│       ├── scripts/
│       │   └── build_push_ecr.sh  # Script to build and push Docker image
│       └── terraform/
│           ├── main.tf            # Infrastructure as code
│           └── terraform.tfvars   # Configuration values
├── lambda_functions/
│   └── proxy/
│       └── index.mjs              # Lambda function for API Gateway
├── glue_scripts/
│   ├── crm_kpi_processor.py       # Glue job for CRM data
│   ├── marketing-kpi.py           # Glue job for marketing KPIs
│   ├── operations_kpis.py         # Glue job for operations KPIs
│   └── sales_glue_script.py       # Glue job for sales KPIs
└── notebook/
    └── test.ipynb                 # Example notebook for data analysis
```

## Troubleshooting

### Common Issues

1. **Connector Not Sending Data**:
   - Check CloudWatch logs for errors
   - Verify network connectivity to source APIs
   - Ensure IAM permissions are correctly configured

2. **Glue Job Failures**:
   - Check job logs in CloudWatch
   - Verify input data schema matches expectations
   - Check for sufficient IAM permissions

3. **Missing Data in Analytics**:
   - Verify Glue crawlers have run successfully
   - Check S3 bucket permissions
   - Ensure data partitioning is correctly configured

## Security Considerations

1. **Data Protection**:
   - S3 buckets configured with encryption at rest
   - HTTPS for all API communications
   - IAM roles with least privilege principle

2. **Access Control**:
   - Fine-grained IAM policies
   - Network security groups for ECS tasks
   - API Gateway with appropriate authorization

## Future Enhancements

1. **Real-time Dashboards**:
   - Implement real-time visualization of KPIs

2. **Machine Learning Integration**:
   - Predictive analytics for customer behavior
   - Anomaly detection for sales patterns

3. **Data Quality Framework**:
   - Automated data quality checks
   - Data lineage tracking

## Contact

For questions or support, please contact the data engineering team.
