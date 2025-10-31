# Docker + Databricks Testing Instructions

**Date:** October 31, 2025  
**Configuration:** Databricks (Production)  
**Status:** ✅ Containers Running & Configured

---

## Current Status

✅ **All Docker Containers are Healthy**

| Container | Status | Configuration |
|-----------|--------|---------------|
| `lending_club_webserver` | ✅ Healthy | Port 3000 |
| `lending_club_daemon` | ✅ Healthy | Background |
| `lending_club_user_code` | ✅ Healthy | Port 4000 |
| `lending_club_postgres` | ✅ Healthy | Port 5432 |

✅ **Databricks Configuration Verified**

```
DATABASE_TYPE=databricks
DBT_TARGET=prod
DATABRICKS_HOST=dbc-4125f268-cbe4.cloud.databricks.com
DATABRICKS_CATALOG=workspace
DATABRICKS_SCHEMA=default
```

---

## Testing via Dagster UI

### Step 1: Access Dagster UI

Open your browser and navigate to:
```
http://localhost:3000
```

You should see the Dagster UI with the Lending Club Pipeline loaded.

### Step 2: Navigate to Assets

1. Click **"Assets"** in the left sidebar
2. You should see all 8 pipeline assets:
   - `customers_raw`
   - `accounts_raw`
   - `staging/stg_customers__cleaned`
   - `staging/stg_accounts__cleaned`
   - `marts/account_summary`
   - `account_summary_csv`
   - `account_summary_parquet`
   - `account_summary_to_databricks` ⭐ (This will run with Databricks!)

### Step 3: Materialize All Assets

1. Click the **"Materialize all"** button (top right corner)
2. A confirmation dialog will appear
3. Click **"Materialize"** to start the pipeline
4. You'll be redirected to the **Run** page

### Step 4: Monitor Execution

Watch the real-time execution in the Dagster UI:

**Expected Timeline:**
- **0-30s:** Ingestion phase
  - `customers_raw` - Loads 10 rows to Databricks
  - `accounts_raw` - Loads 10 rows to Databricks

- **30-70s:** DBT Transformations
  - `stg_customers__cleaned` - Staging transformation
  - `stg_accounts__cleaned` - Staging transformation
  - `account_summary` - Mart with interest calculations
  - **21 DBT tests** execute automatically

- **70-100s:** Output Generation
  - `account_summary_csv` - Exports to CSV (351 bytes)
  - `account_summary_parquet` - Exports to Parquet (4.3 KB)
  - `account_summary_to_databricks` - **Loads 8 rows to Databricks table** ⭐

**Total Expected Duration:** ~90-100 seconds

### Step 5: Verify Success

Look for these indicators:

✅ **Green Checkmarks** next to all 8 assets  
✅ **Run Status:** SUCCESS  
✅ **DBT Tests:** PASS=21 WARN=0 ERROR=0  
✅ **All Assets Materialized:** 8/8

---

## Verification Steps

### 1. Check Output Files in Container

```bash
# Access the container
docker exec -it lending_club_webserver bash

# Inside container, check output files
ls -lh /app/data/outputs/

# Expected output:
# account_summary.csv      351 bytes
# account_summary.parquet  4.3 KB

# View CSV content
head /app/data/outputs/account_summary.csv

# Exit container
exit
```

### 2. Verify Databricks Tables

From your host machine, run:

```bash
docker exec lending_club_webserver python -c "
from databricks import sql
import os

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
        
        # Show sample data
        cursor.execute('SELECT * FROM workspace.default.account_summary LIMIT 3')
        rows = cursor.fetchall()
        print('Sample data:')
        for row in rows:
            print(f'  {row}')
"
```

**Expected Output:**
```
Tables: ['account_summary']
account_summary rows: 8
Sample data:
  Row(customer_id='1', account_id='A001', original_balance=5000.0, ...)
  Row(customer_id='2', account_id='A002', original_balance=15000.0, ...)
  Row(customer_id='3', account_id='A003', original_balance=25000.0, ...)
```

### 3. Run Smoke Tests in Container

```bash
docker exec lending_club_webserver python /app/tests/smoke_test.py
```

**Expected Output:**
```
============================================================
RESULTS: 4 passed, 0 failed, 0 skipped
============================================================
✓ Output Files Exist
✓ Output Data Quality
✓ CSV/Parquet Consistency
✓ Databricks Table Exists
```

---

## What to Look For in Dagster UI

### Asset Details

Click on `account_summary_to_databricks` asset to see:

**Metadata:**
- Table name: `workspace.default.account_summary`
- Row count: 8
- Column count: 6
- Catalog: workspace
- Schema: default
- Execution timestamp
- Number of attempts: 1 (should succeed on first try)

**Preview:**
- Sample data showing customer_id, account_id, balances, interest calculations

### Run Logs

In the run details, look for these log messages:

```
✓ "Reading account data from data/inputs/accounts.csv"
✓ "Successfully loaded 10 account records"
✓ "Writing 10 rows to Databricks table: raw.accounts_raw"
✓ "Successfully wrote 10 rows to raw.accounts_raw"
✓ "Done. PASS=21 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=21"
✓ "Loading 8 rows to Databricks table workspace.default.account_summary"
✓ "Successfully loaded data to workspace.default.account_summary"
✓ "Table details - Rows: 8, Columns: 6"
```

---

## Troubleshooting

### Issue: Databricks Connection Error

**Symptoms:**
- Run fails during ingestion or Databricks load
- Error message: "Connection refused" or "Authentication failed"

**Check:**
```bash
# Verify Databricks credentials in container
docker exec lending_club_webserver env | grep DATABRICKS

# Test connection
docker exec lending_club_webserver python -c "
from databricks import sql
import os
conn = sql.connect(
    server_hostname=os.getenv('DATABRICKS_HOST'),
    http_path=os.getenv('DATABRICKS_HTTP_PATH'),
    access_token=os.getenv('DATABRICKS_TOKEN'),
)
print('✓ Connection successful')
conn.close()
"
```

**Solution:**
1. Verify Databricks SQL Warehouse is running
2. Check token is valid
3. Verify network connectivity
4. Restart containers: `docker-compose restart`

### Issue: DBT Tests Fail

**Symptoms:**
- Run fails during DBT transformation phase
- Error message shows test failures

**Check:**
```bash
# View DBT logs in container
docker exec lending_club_webserver cat /app/dbt_project/target/dbt.log | tail -100
```

**Solution:**
1. Check raw tables were created successfully
2. Verify data quality in raw tables
3. Review specific test failures in logs

### Issue: Assets Not Appearing in UI

**Symptoms:**
- UI shows no assets or incomplete asset list

**Check:**
```bash
# Check user code server logs
docker-compose logs dagster-user-code | tail -50
```

**Solution:**
```bash
# Restart user code server
docker-compose restart dagster-user-code

# Wait 10 seconds and refresh browser
```

---

## Performance Expectations

### Databricks Configuration

| Stage | Expected Duration | Notes |
|-------|------------------|-------|
| Ingestion | 25-35s | Network latency to Databricks |
| DBT Transformations | 35-45s | Includes 21 tests |
| Outputs | 8-12s | CSV, Parquet, Databricks load |
| **Total** | **90-100s** | End-to-end pipeline |

### Comparison with DuckDB

| Metric | DuckDB | Databricks | Difference |
|--------|--------|------------|------------|
| Total Duration | ~20s | ~95s | +375% |
| Ingestion | <1s | ~30s | +3000% |
| Outputs | <1s | ~10s | +1000% |

**Note:** Databricks overhead is expected due to network latency and cloud infrastructure. The trade-off provides enterprise features like scalability, security, and collaboration.

---

## Success Criteria

### ✅ Test Passes If:

- [ ] All 8 assets materialize successfully
- [ ] Run status shows SUCCESS
- [ ] DBT tests show PASS=21 WARN=0 ERROR=0
- [ ] Output files created (CSV and Parquet)
- [ ] Databricks table `workspace.default.account_summary` created with 8 rows
- [ ] No errors in container logs
- [ ] Smoke tests pass (4/4)
- [ ] Data quality checks pass (calculations correct, no nulls)

---

## Next Steps After Successful Test

1. ✅ **Document Results**
   - Capture screenshots from Dagster UI
   - Save run logs
   - Record execution times

2. ✅ **Verify Data Quality**
   - Check calculations in Databricks
   - Verify no data loss
   - Confirm all 8 rows present

3. ✅ **Test Edge Cases** (Optional)
   - Test with invalid data
   - Test with missing files
   - Test connection failures

4. ✅ **Prepare for Production**
   - Set up monitoring
   - Configure alerting
   - Schedule pipeline runs
   - Document operational procedures

---

## Clean Up

### Stop Containers (Keep Data)

```bash
docker-compose down
```

### Stop Containers (Remove All Data)

```bash
docker-compose down -v
```

### Switch Back to DuckDB

1. Edit `.env`:
```properties
DATABASE_TYPE=duckdb
DBT_TARGET=dev
```

2. Restart containers:
```bash
docker-compose down
docker-compose up -d
```

---

## Support & Resources

**Dagster UI:** http://localhost:3000  
**Databricks Workspace:** https://dbc-4125f268-cbe4.cloud.databricks.com  

**Documentation:**
- `DOCKER_DAGSTER_UI_TESTING_GUIDE.md` - General Docker/UI testing
- `PIPELINE_EXECUTION_GUIDE.md` - Complete pipeline guide
- `FINAL_TEST_REPORT.md` - Test results and validation

---

**Ready to Test!** 🚀

Open http://localhost:3000 in your browser and click "Materialize all" to start the pipeline with Databricks configuration.

---

**Last Updated:** October 31, 2025  
**Configuration:** Databricks Production  
**Expected Duration:** ~95 seconds
