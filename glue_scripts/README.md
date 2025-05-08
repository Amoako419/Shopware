# Glue Scripts

This directory contains AWS Glue ETL scripts used for data transformation and KPI calculations in the Shopware Data Pipeline.

## Scripts Overview

- `catalog_to_s3_parquet_job.py`: Transforms catalog data to Parquet format
- `crm_kpi_processor.py`: Processes CRM data and calculates related KPIs
- `marketing-kpi.py`: Computes marketing metrics from web and customer data
- `operations_kpis.py`: Calculates operational KPIs from inventory and POS data
- `sales_glue_script.py`: Processes sales data and computes sales metrics

## Input/Output

### Input Sources
- Bronze layer S3 buckets (raw data)
- Silver layer S3 buckets (processed data)
- Catalog databases

### Output Destinations
- Silver layer S3 buckets (transformed data)
- Gold layer S3 buckets (aggregated KPIs)
- Redshift tables (for reporting)

## Development

### Prerequisites
- Python 3.11+
- AWS Glue libraries
- Access to AWS Glue development endpoints

### Local Testing
1. Set up a Docker container with Glue libraries
2. Use sample data from the `test` directory
3. Run scripts using the Glue development endpoint

## Deployment

Scripts are deployed via AWS Glue Jobs. See main project README for deployment steps.

## Monitoring

- CloudWatch logs for job execution
- Job bookmarks for tracking processed data
- Data quality metrics in CloudWatch