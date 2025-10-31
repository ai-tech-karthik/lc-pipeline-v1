# Docker & Dagster UI Testing Guide

This guide provides instructions for testing the Lending Club Pipeline using Docker containers and the Dagster UI.

---

## Current Status

✅ **Docker Containers Running**

All containers are healthy and operational:

| Container | Status | Port | Purpose |
|-----------|--------|------|---------|
| `lending_club_webserver` | ✅ Healthy | 3000 | Dagster UI & API |
| `lending_club_daemon` | ✅ Healthy | - | Schedules & Sensors |
| `lending_club_user_code` | ✅ Healthy | 4000 | Pipeline Code |
| `lending_club_postgres` | ✅ Healthy | 5432 | Metadata Storage |

---

## Access Dagster UI

### 1. Open Dagster UI in Browser

```
http://localhost:3000
```

The Dagster UI should now be accessible in your web browser.

### 2. Navigate to Assets

Once the UI loads:
1. Click on **"Assets"** in the left sidebar
2. You should see all 8 pipeline assets:
   - `customers_raw`
   - `accounts_raw`
   - `staging/stg_customers__cleaned`
   - `staging/stg_accounts__cleaned`
   - `marts/account_summary`
   - `account_summary_csv`
   - `account_summary_parquet`
   - `account_summary_to_databricks`

---

## Testing via Dagster UI

### Test 1: Materialize All Assets

#### Steps:
1. In the **Assets** view, click **"Materialize all"** button (top right)
2. A dialog will appear - click **"Materialize"** to confirm
3. You'll be redirected to the **Run** page
4. Watch the real-time execution logs

#### Expected Results:
- All 8 assets should materialize successfully
- Total duration: ~20-40 seconds
- DBT tests: 21/21 PASS
- Final status: ✅ SUCCESS

#### What to Look For:
- **Green checkmarks** next to each asset
- **"Materialized"** status for all assets
- **No errors** in the logs
- **DBT test results** showing PASS=21

### Test 2: Materialize Individual Asset

#### Steps:
1. Click on any asset (e.g., `customers_raw`)
2. Click **"Materialize"** button in the asset details page
3. Watch the execution

#### Expected Results:
- Asset materializes successfully
- Downstream assets show as "stale" (need rematerialization)

### Test 3: View Asset Lineage

#### Steps:
1. Click on `marts/account_summary` asset
2. View the **Lineage** tab
3. See the dependency graph

#### Expected Lineage:
```
customers_raw ──┐
                ├──> stg_customers__cleaned ──┐
                                               ├──> account_summary ──> outputs
accounts_raw ───┘                              │
                ├──> stg_accounts__cleaned ────┘
```

### Test 4: View Run History

#### Steps:
1. Click **"Runs"** in the left sidebar
2. View all pipeline executions
3. Click on any run to see detailed logs

#### What to Check:
- Run status (SUCCESS/FAILED)
- Duration
- Assets materialized
- Logs and errors (if any)

### Test 5: View Asset Details

#### Steps:
1. Click on `account_summary_csv` asset
2. View the **Overview** tab
3. Check **Metadata**:
   - Row count
   - File size
   - Execution timestamp
   - Data preview

---

## Testing via Docker CLI

### View Container Logs

```bash
# View all logs
docker-compose logs

# View specific container logs
docker-compose logs dagster-webserver
docker-compose logs dagster-user-code
docker-compose logs dagster-daemon

# Follow logs in real-time
docker-compose logs -f dagster-webserver
```

### Execute Commands in Container

```bash
# Access webserver container
docker exec -it lending_club_webserver bash

# Inside container, run smoke tests
python /app/tests/smoke_test.py

# Check output files
ls -lh /app/data/outputs/

# View DuckDB tables
python -c "import duckdb; conn = duckdb.connect('/app/data/duckdb/lending_club.duckdb'); print(conn.execute('SHOW ALL TABLES').df())"
```

### Restart Containers

```bash
# Restart all containers
docker-compose restart

# Restart specific container
docker-compose restart dagster-webserver
```

### Stop Containers

```bash
# Stop all containers
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

---

## Verification Checklist

### ✅ Pre-Test Checklist
- [ ] Docker Desktop is running
- [ ] All 4 containers are healthy
- [ ] Dagster UI is accessible at http://localhost:3000
- [ ] Input files exist in `data/inputs/` directory

### ✅ During Test Checklist
- [ ] All assets appear in the UI
- [ ] Asset lineage graph is correct
- [ ] Materialize all button works
- [ ] Run starts and shows progress
- [ ] Logs are visible and updating

### ✅ Post-Test Checklist
- [ ] All assets show green checkmarks
- [ ] Run status is SUCCESS
- [ ] DBT tests show PASS=21
- [ ] Output files created in `data/outputs/`
- [ ] No errors in container logs

---

## Expected Test Results

### Successful Run Output

**Assets Materialized:** 8/8
```
✅ customers_raw (10 rows)
✅ accounts_raw (10 rows)
✅ staging/stg_customers__cleaned
✅ staging/stg_accounts__cleaned
✅ marts/account_summary (8 rows)
✅ account_summary_csv (351 bytes)
✅ account_summary_parquet (4.3 KB)
⊘ account_summary_to_databricks (skipped - DuckDB mode)
```

**DBT Tests:**
```
PASS=21 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=21
```

**Output Files:**
```
data/outputs/
├── account_summary.csv      351 bytes
└── account_summary.parquet  4.3 KB
```

---

## Troubleshooting

### Issue: UI Not Loading

**Check:**
```bash
docker-compose ps
curl http://localhost:3000/server_info
```

**Solution:**
```bash
docker-compose restart dagster-webserver
```

### Issue: Assets Not Appearing

**Check:**
```bash
docker-compose logs dagster-user-code
```

**Solution:**
```bash
docker-compose restart dagster-user-code
```

### Issue: Run Fails

**Check:**
1. View run logs in UI
2. Check container logs: `docker-compose logs`
3. Verify input files exist
4. Check environment variables

**Solution:**
```bash
# Check environment
docker exec lending_club_webserver env | grep DATABASE_TYPE

# Verify input files
docker exec lending_club_webserver ls -la /app/data/inputs/
```

### Issue: Permission Errors

**Solution:**
```bash
# Fix permissions on host
chmod -R 777 data/

# Restart containers
docker-compose restart
```

---

## Advanced Testing

### Test with Databricks

1. Update `.env` file:
```properties
DATABASE_TYPE=databricks
DBT_TARGET=prod
```

2. Restart containers:
```bash
docker-compose down
docker-compose up -d
```

3. Materialize assets via UI
4. Verify Databricks table creation

### Test Schedules (Future)

1. Define a schedule in `definitions.py`
2. Enable schedule in UI
3. Watch automatic runs

### Test Sensors (Future)

1. Define a sensor in `definitions.py`
2. Enable sensor in UI
3. Trigger by adding files

---

## Performance Monitoring

### Via Dagster UI

1. Go to **Runs** page
2. View run duration trends
3. Check asset materialization times
4. Monitor resource usage

### Via Docker Stats

```bash
# Monitor container resource usage
docker stats

# View specific container
docker stats lending_club_webserver
```

---

## Clean Up

### Stop Containers (Keep Data)

```bash
docker-compose down
```

### Stop Containers (Remove Data)

```bash
docker-compose down -v
rm -rf data/duckdb/* data/outputs/*
```

### Remove Images

```bash
docker-compose down
docker rmi lending-club-pipeline:latest
```

---

## Next Steps

After successful Docker testing:

1. ✅ Verify all assets materialize successfully
2. ✅ Check output data quality
3. ✅ Review run logs for any warnings
4. ✅ Test with different configurations (DuckDB/Databricks)
5. ✅ Document any issues or improvements
6. ✅ Prepare for production deployment

---

## Screenshots to Capture

For documentation purposes, capture:

1. **Assets Page** - Showing all 8 assets
2. **Asset Lineage** - Dependency graph
3. **Run Page** - Successful run with green checkmarks
4. **Asset Details** - Metadata and preview
5. **Run Logs** - DBT test results

---

## Support

**Dagster UI:** http://localhost:3000  
**API Endpoint:** http://localhost:3000/graphql  
**User Code Server:** http://localhost:4000

**Documentation:**
- Dagster Docs: https://docs.dagster.io
- DBT Docs: https://docs.getdbt.com

---

**Last Updated:** October 30, 2025  
**Docker Compose Version:** 3.8  
**Dagster Version:** 1.5.0+
