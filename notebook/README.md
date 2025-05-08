# Notebooks

This directory contains Jupyter notebooks used for data analysis, exploration, and visualization in the Shopware Data Pipeline project.

## Contents

- `test.ipynb`: Example notebook demonstrating data analysis workflows

## Setup

### Prerequisites
- Python 3.11+
- Jupyter Lab/Notebook
- Required Python packages (see requirements.txt)

### Installation
```bash
pip install -r requirements.txt
jupyter lab
```

## Usage

These notebooks are used for:
- Exploratory data analysis
- KPI validation
- Ad-hoc analysis
- Prototype development
- Data quality checks

## Best Practices

1. Clear all outputs before committing
2. Use relative paths for data files
3. Document all analysis steps
4. Include error handling
5. Regular checkpoint saves

## Data Sources

Notebooks can connect to:
- S3 data lake
- Redshift warehouse
- Local data files
- AWS Athena

## Security Notes

- Don't store credentials in notebooks
- Use AWS credentials from environment
- Avoid exposing sensitive data
- Clear all outputs before sharing