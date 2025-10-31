# Lending Club Pipeline - DuckDB Smoke Test Results

**Date:** October 30, 2025  
**Environment:** DuckDB (Local Development)  
**Status:** ✅ ALL TESTS PASSED

## Test Summary

| Test Category | Status | Details |
|--------------|--------|---------|
| Complete Pipeline Execution | ✅ PASS | All assets materialized successfully |
| DBT Tests | ✅ PASS | 21/21 tests passed (PASS=21 WARN=0 ERROR=0) |
| Dagster Asset Checks | ✅ PASS | All 8 assets materialized without errors |
| Output Data Quality | ✅ PASS | Calculations correct, no nulls |
| Output Files | ✅ PASS | CSV and Parquet files created |
| DuckDB Integration | ✅ PASS | All tables created successfully |

## Pipeline Execution Results

### 1. Data Ingestion (Raw Layer)
- ✅ `customers_raw`: 10 rows ingested to DuckDB (199ms)
- ✅ `accounts_raw`: 10 rows ingested to DuckDB (185ms)

### 2. DBT Transformations
- ✅ `stg_customers__cleaned`: Staging transformation completed
- ✅ `stg_accounts__cleaned`: Staging transformation completed  
- ✅ `account_summary`: Mart created with interest calculations
- ✅ **All 21 DBT tests passed** (data quality, relationships, constraints)
- ⏱️ **Duration:** 18.3 seconds

### 3. Output Assets
- ✅ `account_summary_csv`: 8 rows exported to `data/outputs/account_summary.csv` (351 bytes, 243ms)
- ✅ `account_summary_parquet`: 8 rows exported to `data/outputs/account_summary.parquet` (4.4 KB, 351ms)
- ⊘ `account_summary_to_databricks`: Skipped (DATABASE_TYPE=duckdb)

## Data Quality Verification

### Calculations Verified
- ✅ Interest calculations: `annual_interest = original_balance * interest_rate`
- ✅ New balance calculations: `new_balance = original_balance + annual_interest`
- ✅ No null values in critical columns
- ✅ Correct data types (integers for IDs, floats for amounts)

### Sample Output Data
```
customer_id,account_id,original_balance,interest_rate,annual_interest,new_balance
1,A001,5000.0,0.015,75.0,5075.0
2,A002,15000.0,0.015,225.0,15225.0
3,A003,25000.0,0.02,500.0,25500.0
4,A004,8000.0,0.015,120.0,8120.0
5,A005,22000.0,0.02,440.0,22440.0
7,A007,30000.0,0.02,600.0,30600.0
8,A008,7500.0,0.01,75.0,7575.0
10,A010,50000.0,0.02,1000.0,51000.0
```

### Data Consistency
- ✅ CSV and Parquet outputs contain identical data
- ✅ DuckDB tables match output files
- ✅ 8 rows in all outputs (2 customers filtered out due to missing accounts)

## DBT Test Results

All 21 DBT tests passed successfully:

### Staging Layer Tests
- ✅ `stg_accounts__cleaned`: 9 tests passed
  - Unique account_id
  - Not null checks (account_id, customer_id, balance, account_type)
  - Accepted values for account_type (Savings, Checking)
  - Relationships to raw tables
  - Balance >= 0 constraint

- ✅ `stg_customers__cleaned`: 6 tests passed
  - Unique customer_id
  - Not null checks (customer_id, customer_name, email)
  - Relationships to raw tables

### Mart Layer Tests
- ✅ `account_summary`: 6 tests passed
  - Unique account_id
  - Not null checks (customer_id, account_id, original_balance, interest_rate, annual_interest, new_balance)
  - Relationships to staging tables
  - Custom test: Interest calculation accuracy

## DuckDB Tables Created

```
database        schema          name
lending_club    raw             accounts_raw
lending_club    raw             customers_raw
lending_club    main_staging    stg_accounts__cleaned
lending_club    main_staging    stg_customers__cleaned
lending_club    main_marts      account_summary
```

## Performance Metrics

| Stage | Duration | Rows Processed |
|-------|----------|----------------|
| Ingestion (customers_raw) | 199ms | 10 rows |
| Ingestion (accounts_raw) | 185ms | 10 rows |
| DBT Transformations | 18.3s | 8 rows (final) |
| CSV Export | 243ms | 8 rows |
| Parquet Export | 351ms | 8 rows |
| **Total Pipeline** | **~20s** | **8 rows** |

## Environment Configuration

```properties
DATABASE_TYPE=duckdb
DBT_TARGET=dev
DUCKDB_PATH=/Users/priyakarthik/MyProjects/MyNextJobInterview/LendingClub/Assignment/lc-pipeline-v1/data/duckdb/lending_club.duckdb
DAGSTER_HOME=/Users/priyakarthik/MyProjects/MyNextJobInterview/LendingClub/Assignment/lc-pipeline-v1/dagster_home
```

## Smoke Test Output

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
⊘ SKIPPED: Databricks test only runs when DATABASE_TYPE=databricks

============================================================
RESULTS: 3 passed, 0 failed, 1 skipped
============================================================
```

## Key Findings

### Strengths
- ✅ Fast execution time (~20 seconds total)
- ✅ All data quality tests pass
- ✅ Calculations are accurate
- ✅ No data loss or corruption
- ✅ Consistent output across formats
- ✅ Proper error handling and validation

### Performance
- DuckDB provides excellent performance for local development
- Significantly faster than Databricks for small datasets
- Ideal for development, testing, and CI/CD pipelines

### Data Quality
- All 21 DBT tests pass without warnings or errors
- Interest calculations are mathematically correct
- No null values in critical columns
- Proper data type handling

## Conclusion

✅ **The Lending Club Pipeline is fully operational with DuckDB.**

The pipeline successfully:
- Ingests data from CSV files to DuckDB
- Applies DBT transformations with comprehensive data quality tests
- Produces multiple output formats (CSV, Parquet)
- Maintains data quality and calculation accuracy
- Executes efficiently in a local development environment

The DuckDB configuration is ideal for:
- Local development and testing
- CI/CD pipelines
- Quick iterations and debugging
- Cost-effective development environment

---

**Next Steps:**
- For production deployment, switch to Databricks configuration
- See `PIPELINE_EXECUTION_GUIDE.md` for detailed instructions
- See `SMOKE_TEST_RESULTS.md` for Databricks test results
