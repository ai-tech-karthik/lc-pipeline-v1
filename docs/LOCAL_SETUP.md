# Local Development Setup Guide

This guide explains how to set up and run the LendingClub data pipeline locally using a Python virtual environment.

## Prerequisites

- Python 3.9 or higher
- pip (Python package manager)
- Git

## Quick Start

### 1. Set Up Virtual Environment

Run the setup script to create a virtual environment and install all dependencies:

```bash
bash scripts/setup_local_env.sh
```

This script will:
- Create a Python virtual environment in `venv/`
- Install all required Python packages
- Install DBT dependencies
- Set up the project in editable mode

### 2. Activate Virtual Environment

```bash
source venv/bin/activate
```

You should see `(venv)` in your terminal prompt.

### 3. Create Sample Data (Optional)

If you don't have input data files, create sample data:

```bash
python scripts/create_sample_data.py
```

This creates:
- `data/inputs/Customer.csv` - Sample customer data
- `data/inputs/accounts.csv` - Sample account data

### 4. Run the Full Pipeline

```bash
python scripts/run_pipeline_locally.py
```

This will:
- Materialize all assets from ingestion to outputs
- Create DuckDB database at `data/duckdb/lending_club.duckdb`
- Generate output files:
  - `data/outputs/account_summary.csv`
  - `data/outputs/account_summary.parquet`

## Running Tests

### Run All E2E Tests

```bash
pytest tests/e2e/test_full_pipeline.py -v
```

### Run Specific Test

```bash
pytest tests/e2e/test_full_pipeline.py::TestFullPipeline::test_full_pipeline_execution -v
```

### Run All Tests with Coverage

```bash
pytest tests/ -v --cov=src/lending_club_pipeline --cov-report=html
```

View coverage report:
```bash
open htmlcov/index.html
```

## Manual Step-by-Step Setup

If you prefer to set up manually:

### 1. Create Virtual Environment

```bash
python3 -m venv venv
```

### 2. Activate Virtual Environment

```bash
source venv/bin/activate
```

### 3. Upgrade pip

```bash
pip install --upgrade pip
```

### 4. Install Package

```bash
# Install with dev dependencies
pip install -e ".[dev]"
```

### 5. Install DBT Dependencies

```bash
cd dbt_project
dbt deps
cd ..
```

## Environment Variables

The pipeline uses these environment variables (with defaults):

```bash
export DATABASE_TYPE=duckdb          # Database type (duckdb or databricks)
export DBT_TARGET=dev                # DBT target environment
export DUCKDB_PATH=data/duckdb/lending_club.duckdb  # DuckDB database path
export OUTPUT_PATH=data/outputs      # Output directory for CSV/Parquet files
```

## Directory Structure

```
.
├── data/
│   ├── inputs/          # Input CSV files
│   ├── duckdb/          # DuckDB database files
│   └── outputs/         # Output CSV and Parquet files
├── dbt_project/         # DBT transformation models
├── src/                 # Source code
│   └── lending_club_pipeline/
│       ├── assets/      # Dagster assets
│       ├── resources/   # Dagster resources
│       └── definitions.py
├── tests/               # Test files
│   ├── unit/           # Unit tests
│   ├── integration/    # Integration tests
│   └── e2e/            # End-to-end tests
├── scripts/            # Utility scripts
└── venv/               # Virtual environment (created by setup)
```

## Running Individual Components

### Run Only Ingestion

```python
from dagster import materialize, AssetSelection
from lending_club_pipeline.definitions import defs

result = materialize(
    AssetSelection.groups("ingestion"),
    resources=defs.resources,
)
```

### Run Only Transformations

```python
from dagster import materialize, AssetSelection
from lending_club_pipeline.definitions import defs

result = materialize(
    AssetSelection.groups("staging", "intermediate", "marts"),
    resources=defs.resources,
)
```

### Run Only Outputs

```python
from dagster import materialize, AssetSelection
from lending_club_pipeline.definitions import defs

result = materialize(
    AssetSelection.groups("outputs"),
    resources=defs.resources,
)
```

## Troubleshooting

### Issue: `ModuleNotFoundError`

**Solution:** Make sure you've installed the package in editable mode:
```bash
pip install -e ".[dev]"
```

### Issue: `FileNotFoundError` for input files

**Solution:** Create sample data:
```bash
python scripts/create_sample_data.py
```

### Issue: DBT errors about missing tables

**Solution:** Run ingestion assets first to populate the database:
```bash
python -c "
from dagster import materialize, AssetSelection
from lending_club_pipeline.definitions import defs
materialize(AssetSelection.groups('ingestion'), resources=defs.resources)
"
```

### Issue: Permission denied on scripts

**Solution:** Make scripts executable:
```bash
chmod +x scripts/*.sh scripts/*.py
```

## Deactivating Virtual Environment

When you're done working:

```bash
deactivate
```

## Cleaning Up

To remove the virtual environment and generated files:

```bash
# Remove virtual environment
rm -rf venv/

# Remove generated data (optional)
rm -rf data/duckdb/ data/outputs/

# Remove DBT artifacts (optional)
rm -rf dbt_project/target/ dbt_project/dbt_packages/
```

## Next Steps

- View the [Architecture Documentation](../README.md)
- Explore [DBT Models](../dbt_project/models/)
- Review [Asset Definitions](../src/lending_club_pipeline/assets/)
- Check [Test Coverage](../tests/)
