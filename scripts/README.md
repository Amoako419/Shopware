# Scripts

This directory contains utility scripts used for deployment, maintenance, and automation tasks in the Shopware Data Pipeline.

## Purpose

These scripts automate common tasks such as:
- Deployment of AWS resources
- Data quality checks
- Maintenance operations
- Monitoring setup

## Usage

Each script is documented with its purpose and usage instructions. Before running any script:
1. Review the script contents
2. Check required permissions
3. Test in a non-production environment first

## Security

- Scripts use AWS credentials from environment
- Sensitive data handled via AWS Secrets Manager
- Logging for audit purposes
- Error handling and rollback capabilities

## Best Practices

- Use appropriate AWS profiles
- Follow the principle of least privilege
- Log all actions
- Include error handling
- Add proper documentation