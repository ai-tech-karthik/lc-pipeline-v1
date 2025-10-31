# Databricks Setup Checklist

Quick checklist for setting up Databricks integration.

## ☐ Step 1: Get Databricks Credentials

- [ ] Log into Databricks: https://dbc-4125f268-cbe4.cloud.databricks.com
- [ ] Generate Personal Access Token
  - Username → User Settings → Developer → Access tokens
  - Click "Generate new token"
  - Copy the token
- [ ] Get SQL Warehouse HTTP Path
  - SQL Warehouses → Your warehouse → Connection Details
  - Copy HTTP Path (e.g., `/sql/1.0/warehouses/xxxxx`)

## ☐ Step 2: Update .env File

Edit `.env` and uncomment/update these lines:

```bash
DATABRICKS_HOST=dbc-4125f268-cbe4.cloud.databricks.com
DATABRICKS_TOKEN=dapi...  # Paste your token here
DATABRICKS_CATALOG=hive_metastore  # For Community Edition
DATABRICKS_SCHEMA=default  # Or create custom schema
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/...  # Paste your HTTP path
```

## ☐ Step 3: Test Connection

```bash
python3 scripts/test_databricks_connection.py
```

Expected: ✅ Connection test PASSED

## ☐ Step 4: Switch to Databricks Mode

Update `.env`:
```bash
DATABASE_TYPE=databricks
DBT_TARGET=prod
```

## ☐ Step 5: Restart Services

```bash
docker-compose restart
```

Wait for healthy status:
```bash
docker-compose ps
```

## ☐ Step 6: Run Pipeline

1. Open http://localhost:3000
2. Go to Assets tab
3. Click "Materialize all"
4. Wait for completion

## ☐ Step 7: Verify in Databricks

Run in Databricks SQL Editor:
```sql
SELECT * FROM hive_metastore.default.account_summary LIMIT 10;
```

## Quick Reference

### Your Databricks Info
- **Host**: dbc-4125f268-cbe4.cloud.databricks.com
- **Catalog**: hive_metastore (Community Edition)
- **Schema**: default (or custom)
- **Token**: Generate in User Settings
- **HTTP Path**: Get from SQL Warehouse Connection Details

### Common Commands

```bash
# Test connection
python3 scripts/test_databricks_connection.py

# Restart services
docker-compose restart

# View logs
docker-compose logs -f

# Check service status
docker-compose ps
```

### Troubleshooting

**Connection fails?**
- Check token is valid
- Verify SQL Warehouse is running
- Confirm HTTP path is correct

**Schema not found?**
- Use `hive_metastore.default` for Community Edition
- Or create schema in Databricks first

**Permission denied?**
- Check token permissions
- Verify user has access to SQL Warehouse

## Done! 🎉

Once all steps are complete, your pipeline will:
- ✅ Ingest data locally
- ✅ Transform with DBT
- ✅ Export to CSV/Parquet
- ✅ Load to Databricks

Access your data in Databricks for BI tools, dashboards, and analytics!
