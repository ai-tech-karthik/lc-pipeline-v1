# Lending Club Pipeline - Execution Guide

This guide provides step-by-step instructions for running the complete Lending Club data pipeline in both local (DuckDB) and production (Databricks) environments.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Running the Pipeline - DuckDB (Local)](#running-the-pipeline---duckdb-local)
- [Running the Pipeline - Databricks (Production)](#running-the-pipeline---databricks-production)
- [Running Smoke Tests](#running-smoke-tests)
- [Troubleshooting](#troubleshooting)
- [Pipeline Architecture](#pipeline-architecture)

---

## Prerequisites

### Required Software
- Python 3.12+
- pip or uv (Python package manager)
- Git

### Required Python Packages
All dependencies are listed in `requirements.txt`. Install them with:
```bash
pip install -r requirements.txt
```

Key packages:
- `dagster` - Orchestration framework
- `dbt-core` - Data transformation
- `dbt-duckdb` - DuckDB adapter for DBT
- `dbt-databricks` - Databricks adapter for DBT
- `databricks-sql-connector` - Databricks connectivity
- `pandas` - Data manipulation
- `pyarrow` - Parquet file support

---

## Environment Setup

### 1. Clone the Repository
```bash
git clone <repository-url>
cd lc-pipeline-v1
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the `.env.example` to `.env` and configure based on your target environment:

```bash
cp .env.example .env
```

#### For DuckDB (Local Development):
```properties
# Environment Configuration
ENVIRONMENT=dev

# Database Configuration
DATABASE_TYPE=duckdb
DUCKDB_PATH=/absolute/path/to/lc-pipeline-v1/data/duckdb/lending_club.duckdb

# DBT Configuration
DBT_TARGET=dev

# Output Configuration
OUTPUT_PATH=data/outputs

# Dagster Configuration
DAGSTER_HOME=/absolute/path/to/lc-pipeline-v1/dagster_home
```

#### For Databricks (Production):
```properties
# Environment Configuration
ENVIRONMENT=prod

# Database Configuration
DATABASE_TYPE=databricks

# DBT Configuration
DBT_TARGET=prod

# Output Configuration
OUTPUT_PATH=data/outputs

# Databricks Configuration
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-databricks-token
DATABRICKS_CATALOG=workspace
DATABRICKS_SCHEMA=default
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id

# Dagster Configuration
DAGSTER_HOME=/absolute/path/to/lc-pipeline-v1/dagster_home
```

**Important Notes:**
- Use **absolute paths** for `DUCKDB_PATH` and `DAGSTER_HOME`
- Never commit `.env` file with real credentials to version control
- For Databricks setup, see `DATABRICKS_SETUP_CHECKLIST.md`

### 5. Create Required Directories
```bash
mkdir -p data/duckdb
mkdir -p data/outputs
mkdir -p data/inputs
mkdir -p dagster_home
```

### 6. Add Input Data Files
Place your input CSV files in `data/inputs/`:
- `Customer.csv` - Customer information
- `accounts.csv` - Account information

---

## Running the Pipeline - DuckDB (Local)

### Step 1: Configure for DuckDB
Ensure your `.env` file is configured for DuckDB:
```properties
DATABASE_TYPE=duckdb
DBT_TARGET=dev
DUCKDB_PATH=/absolute/path/to/lc-pipeline-v1/data/duckdb/lending_club.duckdb
```

### Step 2: Clean Previous Runs (Optional)
```bash
rm -f data/duckdb/lending_club.duckdb
rm -f data/outputs/*
```

### Step 3: Run the Complete Pipeline
```bash
export DAGSTER_HOME=/absolute/path/to/lc-pipeline-v1/dagster_home
dagster asset materialize --select '*' -m src.lending_club_pipeline.definitions
```

### Step 4: Verify the Results
Check that all assets were materialized successfully:
```bash
# Check output files
ls -lh data/outputs/

# Expected files:
# - account_summary.csv
# - account_summary.parquet

# Verify DuckDB tables
python -c "import duckdb; conn = duckdb.connect('data/duckdb/lending_club.duckdb'); print(conn.execute('SHOW ALL TABLES').df())"
```

### Step 5: Run Smoke Tests
```bash
python tests/smoke_test.py
```

Expected output:
```
============================================================
LENDING CLUB PIPELINE - SMOKE TESTS
============================================================

Running: Output Files Exist
------------------------------------------------------------
✓ Output files exist and are not empty

Running: Output Data Quality
------------------------------------------------------------
✓ Data quality checks passed for 8 rows
  - No null values in critical columns
  - Interest calculations are correct
  - New balance calculations are correct

Running: CSV/Parquet Consistency
------------------------------------------------------------
✓ CSV and Parquet outputs are consistent (8 rows)

Running: Databricks Table Exists
------------------------------------------------------------
⊘ Skipping Databricks test - DATABASE_TYPE is duckdb

============================================================
RESULTS: 3 passed, 0 failed, 1 skipped
============================================================
```

---

## Running the Pipeline - Databricks (Production)

### Step 1: Configure Databricks
1. Follow the setup instructions in `DATABRICKS_SETUP_CHECKLIST.md`
2. Ensure your Databricks workspace is configured with:
   - SQL Warehouse created and running
   - Catalog and schema created
   - Access token generated

### Step 2: Configure for Databricks
Update your `.env` file:
```properties
DATABASE_TYPE=databricks
DBT_TARGET=prod
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token
DATABRICKS_CATALOG=workspace
DATABRICKS_SCHEMA=default
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
```

### Step 3: Test Databricks Connection
```bash
python -c "from databricks import sql; import os; from dotenv import load_dotenv; load_dotenv(); conn = sql.connect(server_hostname=os.getenv('DATABRICKS_HOST'), http_path=os.getenv('DATABRICKS_HTTP_PATH'), access_token=os.getenv('DATABRICKS_TOKEN')); print('✓ Connection successful'); conn.close()"
```

### Step 4: Run the Complete Pipeline
```bash
export DAGSTER_HOME=/absolute/path/to/lc-pipeline-v1/dagster_home
dagster asset materialize --select '*' -m src.lending_club_pipeline.definitions
```

### Step 5: Verify the Results
```bash
# Check output files (CSV and Parquet are still created locally)
ls -lh data/outputs/

# Verify Databricks tables using Python
python -c "
from databricks import sql
import os
from dotenv import load_dotenv

load_dotenv()

with sql.connect(
    server_hostname=os.getenv('DATABRICKS_HOST'),
    http_path=os.getenv('DATABRICKS_HTTP_PATH'),
    access_token=os.getenv('DATABRICKS_TOKEN'),
) as conn:
    with conn.cursor() as cursor:
        # Check tables
        cursor.execute('SHOW TABLES IN workspace.default')
        tables = cursor.fetchall()
        print('Tables:', [t[1] for t in tables])
        
        # Check row count
        cursor.execute('SELECT COUNT(*) FROM workspace.default.account_summary')
        count = cursor.fetchone()[0]
        print(f'account_summary rows: {count}')
"
```

### Step 6: Run Smoke Tests
```bash
python tests/smoke_test.py
```

Expected output:
```
============================================================
LENDING CLUB PIPELINE - SMOKE TESTS
============================================================

Running: Output Files Exist
------------------------------------------------------------
✓ Output files exist and are not empty

Running: Output Data Quality
------------------------------------------------------------
✓ Data quality checks passed for 8 rows
  - No null values in critical columns
  - Interest calculations are correct
  - New balance calculations are correct

Running: CSV/Parquet Consistency
------------------------------------------------------------
✓ CSV and Parquet outputs are consistent (8 rows)

Running: Databricks Table Exists
------------------------------------------------------------
✓ Databricks table exists with 8 rows

============================================================
RESULTS: 4 passed, 0 failed, 0 skipped
============================================================
```

---

## Running Smoke Tests

The smoke test suite validates:
- Output files exist and are not empty
- Data quality (correct calculations, no nulls)
- CSV and Parquet consistency
- Databricks table exists (when DATABASE_TYPE=databricks)

### Run All Tests
```bash
python tests/smoke_test.py
```

### Run Individual Test Functions
```python
from tests.smoke_test import test_output_files_exist, test_output_data_quality

test_output_files_exist()
test_output_data_quality()
```

---

## Troubleshooting

### Common Issues

#### 1. DuckDB Path Issues
**Error:** `IO Error: Cannot open file`

**Solution:** Ensure `DUCKDB_PATH` uses an absolute path:
```bash
# Wrong
DUCKDB_PATH=../data/duckdb/lending_club.duckdb

# Correct
DUCKDB_PATH=/Users/username/project/lc-pipeline-v1/data/duckdb/lending_club.duckdb
```

#### 2. Dagster Home Path Issues
**Error:** `$DAGSTER_HOME must be an absolute path`

**Solution:** Use absolute path for `DAGSTER_HOME`:
```bash
export DAGSTER_HOME=/absolute/path/to/lc-pipeline-v1/dagster_home
```

#### 3. Databricks Connection Errors
**Error:** `Connection refused` or `Authentication failed`

**Solution:**
- Verify your Databricks token is valid
- Ensure SQL Warehouse is running
- Check firewall/network settings
- Verify HTTP path format: `/sql/1.0/warehouses/<warehouse-id>`

#### 4. DBT Tests Failing
**Error:** `ERROR creating sql view model`

**Solution:**
- Ensure raw tables exist before running DBT
- Check that ingestion assets ran successfully
- Verify database connection settings

#### 5. Missing Input Files
**Error:** `FileNotFoundError: data/inputs/Customer.csv`

**Solution:**
- Ensure input CSV files are in `data/inputs/` directory
- Check file names match exactly: `Customer.csv` and `accounts.csv`

### Debug Mode

Run pipeline with verbose logging:
```bash
dagster asset materialize --select '*' -m src.lending_club_pipeline.definitions --log-level DEBUG
```

View DBT logs:
```bash
cat dbt_project/target/dbt.log
```

---

## Pipeline Architecture

### Pipeline Stages

```
┌─────────────────┐
│  Input CSVs     │
│  (data/inputs)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  1. Ingestion   │  ← Dagster Assets (customers_raw, accounts_raw)
│  Raw Layer      │    Loads CSV → Database (DuckDB or Databricks)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  2. DBT Trans   │  ← DBT Models
│  Staging Layer  │    - stg_customers__cleaned
│                 │    - stg_accounts__cleaned
└────────┬────────┘    Data cleaning & normalization
         │
         ▼
┌─────────────────┐
│  3. DBT Trans   │  ← DBT Models
│  Mart Layer     │    - account_summary
│                 │    Interest calculations & joins
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  4. Outputs     │  ← Dagster Assets
│                 │    - account_summary_csv
│                 │    - account_summary_parquet
│                 │    - account_summary_to_databricks (prod only)
└─────────────────┘
```

### Asset Dependencies

```
customers_raw ──┐
                ├──> dbt_transformations ──> account_summary_csv
accounts_raw ───┘         │                  account_summary_parquet
                          │                  account_summary_to_databricks
                          │
                          └──> DBT Models:
                               - stg_customers__cleaned
                               - stg_accounts__cleaned
                               - account_summary
```

### Data Quality Tests

DBT runs 21 tests automatically:
- **Staging Layer (15 tests)**
  - Unique constraints
  - Not null checks
  - Accepted values
  - Relationships
  - Custom constraints (balance >= 0)

- **Mart Layer (6 tests)**
  - Unique constraints
  - Not null checks
  - Relationships
  - Custom test (interest calculation accuracy)

---

## Performance Metrics

### DuckDB (Local)
- **Total Pipeline Duration:** ~20 seconds
- **Ingestion:** ~0.4 seconds (20 rows)
- **DBT Transformations:** ~18 seconds (21 tests)
- **Outputs:** ~0.6 seconds (3 formats)

### Databricks (Production)
- **Total Pipeline Duration:** ~80 seconds
- **Ingestion:** ~35 seconds (20 rows)
- **DBT Transformations:** ~36 seconds (21 tests)
- **Outputs:** ~9 seconds (3 formats)

---

## Additional Resources

- **Databricks Setup:** See `DATABRICKS_SETUP_CHECKLIST.md`
- **Smoke Test Results:** See `SMOKE_TEST_RESULTS.md`
- **DBT Documentation:** Run `dbt docs generate && dbt docs serve` in `dbt_project/`
- **Dagster UI:** Run `dagster dev -m src.lending_club_pipeline.definitions` for web interface

---

## Support

For issues or questions:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review logs in `dbt_project/target/dbt.log`
3. Check Dagster logs in `dagster_home/`
4. Verify environment configuration in `.env`

---

**Last Updated:** October 30, 2025  
**Pipeline Version:** 1.0.0
