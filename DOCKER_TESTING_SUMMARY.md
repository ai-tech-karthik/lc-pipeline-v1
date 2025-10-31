# Docker Testing Summary

**Date:** October 31, 2025  
**Status:** ✅ READY FOR TESTING

---

## Quick Start

### Current Configuration: Databricks (Production)

**Access Dagster UI:**
```
http://localhost:3000
```

**Container Status:**
```bash
docker-compose ps
```

All 4 containers are healthy and running:
- ✅ `lending_club_webserver` (Port 3000)
- ✅ `lending_club_daemon`
- ✅ `lending_club_user_code` (Port 4000)
- ✅ `lending_club_postgres` (Port 5432)

---

## Testing Instructions

### Option 1: Test via Dagster UI (Recommended)

1. **Open Browser:** http://localhost:3000
2. **Click:** Assets → Materialize all
3. **Watch:** Real-time execution (~95 seconds)
4. **Verify:** All 8 assets show green checkmarks

**See:** `DOCKER_DATABRICKS_TEST_INSTRUCTIONS.md` for detailed steps

### Option 2: Test via CLI

```bash
# Run smoke tests in container
docker exec lending_club_webserver python /app/tests/smoke_test.py

# Verify Databricks tables
docker exec lending_club_webserver python -c "
from databricks import sql
import os
with sql.connect(
    server_hostname=os.getenv('DATABRICKS_HOST'),
    http_path=os.getenv('DATABRICKS_HTTP_PATH'),
    access_token=os.getenv('DATABRICKS_TOKEN'),
) as conn:
    with conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM workspace.default.account_summary')
        print(f'Rows: {cursor.fetchone()[0]}')
"
```

---

## What to Expect

### Successful Run

**Duration:** ~95 seconds

**Assets Materialized:** 8/8
- ✅ customers_raw (10 rows → Databricks)
- ✅ accounts_raw (10 rows → Databricks)
- ✅ stg_customers__cleaned
- ✅ stg_accounts__cleaned
- ✅ account_summary (8 rows)
- ✅ account_summary_csv (351 bytes)
- ✅ account_summary_parquet (4.3 KB)
- ✅ account_summary_to_databricks (8 rows → Databricks table)

**DBT Tests:** PASS=21 WARN=0 ERROR=0

**Output Files:**
```
/app/data/outputs/
├── account_summary.csv
└── account_summary.parquet
```

**Databricks Tables:**
```
workspace.default.account_summary (8 rows)
```

---

## Switching Configurations

### Switch to DuckDB (Local)

1. **Stop containers:**
```bash
docker-compose down
```

2. **Edit `.env`:**
```properties
DATABASE_TYPE=duckdb
DBT_TARGET=dev
```

3. **Start containers:**
```bash
docker-compose up -d
```

4. **Test:** http://localhost:3000

### Switch to Databricks (Production)

1. **Stop containers:**
```bash
docker-compose down
```

2. **Edit `.env`:**
```properties
DATABASE_TYPE=databricks
DBT_TARGET=prod
```

3. **Start containers:**
```bash
docker-compose up -d
```

4. **Test:** http://localhost:3000

---

## Verification Commands

### Check Container Status
```bash
docker-compose ps
```

### View Logs
```bash
# All containers
docker-compose logs

# Specific container
docker-compose logs dagster-webserver

# Follow logs
docker-compose logs -f
```

### Check Configuration
```bash
docker exec lending_club_webserver env | grep -E "(DATABASE_TYPE|DBT_TARGET)"
```

### Access Container Shell
```bash
docker exec -it lending_club_webserver bash
```

---

## Troubleshooting

### Containers Not Starting

```bash
# Check logs
docker-compose logs

# Rebuild images
docker-compose build --no-cache
docker-compose up -d
```

### UI Not Loading

```bash
# Check webserver status
docker-compose ps dagster-webserver

# Restart webserver
docker-compose restart dagster-webserver

# Check if port is available
curl http://localhost:3000/server_info
```

### Assets Not Appearing

```bash
# Restart user code server
docker-compose restart dagster-user-code

# Wait 10 seconds
sleep 10

# Refresh browser
```

---

## Documentation

### Detailed Guides

1. **`DOCKER_DATABRICKS_TEST_INSTRUCTIONS.md`**
   - Step-by-step Databricks testing
   - Verification procedures
   - Troubleshooting

2. **`DOCKER_DAGSTER_UI_TESTING_GUIDE.md`**
   - General Docker/UI testing
   - DuckDB configuration
   - Advanced testing

3. **`PIPELINE_EXECUTION_GUIDE.md`**
   - Complete pipeline documentation
   - Both environments
   - Performance metrics

4. **`FINAL_TEST_REPORT.md`**
   - Test results
   - Data quality validation
   - Production readiness

---

## Clean Up

### Stop Containers (Keep Data)
```bash
docker-compose down
```

### Stop Containers (Remove Data)
```bash
docker-compose down -v
```

### Remove Images
```bash
docker-compose down
docker rmi lending-club-pipeline:latest
```

---

## Next Steps

1. ✅ **Test with Databricks** (Current Configuration)
   - Open http://localhost:3000
   - Click "Materialize all"
   - Verify all 8 assets succeed
   - Check Databricks table creation

2. ✅ **Test with DuckDB** (Optional)
   - Switch configuration
   - Restart containers
   - Run pipeline
   - Compare performance

3. ✅ **Document Results**
   - Capture screenshots
   - Save execution logs
   - Record any issues

4. ✅ **Prepare for Production**
   - Review test results
   - Set up monitoring
   - Configure schedules
   - Deploy to production

---

## Support

**Dagster UI:** http://localhost:3000  
**Container Logs:** `docker-compose logs`  
**Documentation:** See files listed above

---

**Status:** ✅ Ready for Testing  
**Configuration:** Databricks (Production)  
**Action Required:** Open http://localhost:3000 and click "Materialize all"

🚀 **Happy Testing!**
