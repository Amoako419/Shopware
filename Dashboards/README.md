# Dashboards

This directory contains Power BI dashboard files for visualizing KPIs and metrics from the Shopware Data Pipeline.

## Files

- `marketing.pbix`: Marketing team dashboard with customer engagement metrics, web traffic analysis, and campaign performance

## Setup

### Prerequisites
- Power BI Desktop
- Access to Redshift cluster
- Appropriate IAM credentials

### Configuration

1. Open the .pbix file in Power BI Desktop
2. Update the data source connection strings
3. Refresh the data to validate connectivity

## Data Sources

- Redshift tables containing KPIs
- S3 data lake (via Athena)
- Direct queries to processed data

## Refresh Schedule

- Marketing KPIs: Every 15 minutes
- Sales data: Hourly
- Inventory metrics: Every 30 minutes

## Security

- Row-level security configured for team access
- Credentials managed via Power BI service
- Data refreshes use service principal authentication