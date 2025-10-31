# LendingClub Data Platform Engineering - Complete Project Design

## Executive Summary

This document outlines a **production-grade data pipeline** for the LendingClub take-home assignment that demonstrates scalable architecture patterns while remaining demo-able locally. The design applies modern data stack best practices: Dagster for orchestration, DBT for transformation, and a local SQLite backend that scales to Databricks in production by changing only configuration.

**Key Design Principle:** Build once, deploy anywhere. The same codebase runs locally on sample data using Docker + SQLite, or in production on Databricks processing millions of records. Zero code changes—only configuration switches environments.

---

## Part 1: High-Level Architecture

### System Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    DAGSTER ORCHESTRATOR                         │
│  (Manages pipeline execution, scheduling, monitoring)           │
└──────────────┬──────────────────────────────────────────────────┘
               │
         ┌─────┴─────┐
         │           │
    ┌────▼─────┐  ┌──▼──────┐
    │ Ingest   │  │ Transform│
    │  Asset   │  │  Assets  │
    └────┬─────┘  └──┬───────┘
         │           │
    ┌────▼───────────▼────┐
    │   DBT Models        │
    │  (3-layer arch)     │
    └────┬────────────────┘
         │
    ┌────▼──────────────────┐
    │  Data Output Layer    │
    │  • Local CSV          │
    │  • Databricks Tables  │
    │  • Local SQLite       │
    └───────────────────────┘
```

### Design Rationale

**Why this architecture?**

1. **Separation of Concerns** - Dagster handles orchestration (when/how to run), DBT handles transformation (what business logic to apply), storage adapts to environment
2. **Testability** - Each layer tests independently; DBT models have unit tests, Dagster assets have integration tests
3. **Maintainability** - Transformation logic stays in SQL (DBT), orchestration logic in Python (Dagster), clear boundaries prevent tangling
4. **Scalability** - Partitioned assets enable processing millions of records by adding partitions; incremental models prevent full table rewrites

---

## Part 2: Technology Stack Decisions

### 1. Orchestration: Dagster (✓ Required, but excellent choice)

**Why Dagster over alternatives:**

| Criterion | Dagster | Airflow | Prefect |
|-----------|---------|---------|---------|
| **Asset-centric** | ✓ Primary | ✗ Secondary | ✓ Modern |
| **DBT Integration** | ✓✓✓ Best-in-class | ✓ OK | ✓ Good |
| **Local Dev** | ✓ Excellent | ✗ Complex | ✓ Good |
| **Production Scale** | ✓ Kubernetes | ✓ Kubernetes | ✓ Cloud-native |
| **Learning Curve** | ✓ Steep but rewarding | ✗ Very steep | ✓ Moderate |
| **Interview Impression** | ✓✓✓ Modern, impressive | ✓✓ Industry standard | ✓✓ Emerging |

**Specific advantages for this project:**
- **DBT integration via @dbt_assets decorator** - Single function materializes all DBT models as Dagster assets with automatic dependency mapping
- **Local development** - `dagster dev` runs the entire stack locally with hot reload
- **Asset lineage visualization** - Interview panel can see data flow graphically
- **Production deployment ready** - Docker to Kubernetes with minimal changes
- **Comprehensive testing** - Python's pytest integrates seamlessly

**Decision: Use Dagster for orchestration**

---

### 2. Transformation: DBT (✓ Required, perfect fit)

**Why DBT for data transformation:**

The assignment requires "cleaning, parsing, filtering, joining, aggregating, schema changes"—exactly what DBT excels at. Alternatives like Spark or Pandas would require:
- Building custom validation logic (DBT provides `unique`, `not_null`, `relationships` tests)
- Managing complex dependency tracking (DBT does this automatically)
- Writing deployment code (DBT is deployment-ready)

**DBT Architecture: Three-Layer Medallion**

```
┌─────────────────────────────────┐
│  staging/                       │
│  • stg_customers__cleaned       │ ← Clean, rename, cast types
│  • stg_accounts__cleaned        │
├─────────────────────────────────┤
│  intermediate/                  │
│  • int_accounts__with_customer  │ ← Join with customer data
│  • int_savings_only             │ ← Filter to savings accounts
├─────────────────────────────────┤
│  marts/                         │
│  • account_summary              │ ← Final business-ready output
│    (customer_id, account_id,    │
│     balance, interest_rate,     │
│     annual_interest, new_balance)
└─────────────────────────────────┘
```

**Why three layers?**

1. **Staging** - Each source gets one staging model. Light transformations only (trim whitespace, normalize casing, convert types). Materializes as **views** (no storage overhead).
   - `stg_customers__cleaned.sql`: Normalize HasLoan to boolean, trim names
   - `stg_accounts__cleaned.sql`: Parse balance as decimal, normalize account type

2. **Intermediate** - Purpose-specific logic. Materializes as **ephemeral models** (intermediate representation, not stored).
   - `int_accounts__with_customer.sql`: Join accounts with customers, include loan flag
   - `int_savings_accounts_only.sql`: Filter to savings account type

3. **Marts** - Final business-ready tables. Materializes as **tables** (optimized for query performance).
   - `account_summary.sql`: Calculate interest, format final report

**Advantages of this structure:**
- ✓ Quality gates at each layer (all staging tested before intermediate)
- ✓ Reusable components (other models reference staging)
- ✓ Clear governance (who owns which layer)
- ✓ Easy debugging (isolate issues to specific layer)
- ✓ Scales naturally (add intermediate layer without touching staging/marts)

**Decision: Use three-layer DBT architecture with views → ephemeral → tables materialization**

---

### 3. Backend Storage for Demo: DuckDB (Local) / Databricks (Production)

**Why not SQLite for local dev?**

| Aspect | DuckDB | SQLite | Databricks |
|--------|--------|--------|-----------|
| **OLAP optimized** | ✓✓✓ | ✗ OLTP | ✓✓✓ |
| **SQL dialect** | ✓ Standard | ✓ Standard | ✓ Spark SQL |
| **CSV read** | ✓ Native | ✗ Needs plugin | ✓ Native |
| **Local performance** | ✓ Fast | ✓ OK | N/A |
| **Parquet support** | ✓ Native | ✗ Plugin | ✓ Native |
| **Production upgrade** | ~ Different | ~ Different | ✓ Same provider |

**Decision: Use DuckDB locally, Databricks in production**

**Why this combination:**
1. DuckDB is **OLAP-optimized** (perfect for analytics), runs in-process (no server), and reads CSV natively
2. Databricks Free Edition works identically to production (no surprises when scaling)
3. DBT adapters are first-class for both (dbt-duckdb and dbt-databricks)
4. Easy migration path: same SQL, different adapter configuration

---

### 4. Container Runtime: Docker Compose (Demo) / Kubernetes (Production)

**Local Development Setup:**

```yaml
services:
  # Dagster webserver
  dagster-webserver:
    image: dagster:latest
    
  # Dagster daemon (schedules/sensors)
  dagster-daemon:
    image: dagster:latest
    
  # User code launcher
  dagster-user-code:
    build: .
    
  # Metadata storage (PostgreSQL)
  postgres:
    image: postgres:15
```

**Why Docker Compose for local?**
- ✓ Reproduces multi-container orchestration locally
- ✓ Easy dependency management (postgres waits for Dagster)
- ✓ Single `docker-compose up` starts everything
- ✓ Volumes enable code hot-reload during development

**Production Deployment:**

Kubernetes with Dagster Helm chart:
- Namespace-based environment isolation (dev, staging, prod)
- Each pipeline run becomes a Kubernetes Job
- Resources constrained per asset (CPU/memory requests/limits)
- Secrets mounted from Kubernetes Secrets
- Logs persisted to S3 for long-term retention

**Decision: Docker Compose for local development, Kubernetes for production**

---

### 5. Output Formats

**Local (Demo) Outputs:**
- `account_summary.csv` - CSV file for inspection
- `account_summary.parquet` - Parquet for efficient storage
- SQLite database with schema mirroring production

**Production Outputs:**
- Databricks Delta table `account_summary`
- Parquet files on S3 for archive
- Optional downstream ML model serving

**Decision: Support both CSV and Parquet locally, Delta tables on Databricks**

---

## Part 3: Project Structure

```
lending-club-pipeline/
│
├── README.md                          # Setup and run instructions
├── docker-compose.yml                 # Local dev container orchestration
├── Dockerfile                         # Custom image with dependencies
├── .env.example                       # Environment variables template
├── requirements.txt                   # Python dependencies
│
├── src/
│   └── lending_club_pipeline/
│       ├── __init__.py
│       ├── definitions.py             # Main Dagster definitions
│       │
│       ├── assets/
│       │   ├── __init__.py
│       │   ├── ingestion.py          # Read CSV inputs → staged format
│       │   └── databricks_loader.py  # Load to Databricks (production)
│       │
│       ├── resources/
│       │   ├── __init__.py
│       │   ├── duckdb_resource.py    # DuckDB connection (local)
│       │   ├── databricks_resource.py # Databricks connection (prod)
│       │   └── config.py             # Environment config loading
│       │
│       ├── io_managers/
│       │   ├── __init__.py
│       │   └── parquet_manager.py    # Store/retrieve Parquet files
│       │
│       ├── sensors/
│       │   └── data_quality_alerts.py
│       │
│       └── utils/
│           ├── __init__.py
│           └── data_validation.py    # Custom validation logic
│
├── dbt_project/                       # DBT project directory
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_customers__cleaned.sql
│   │   │   └── stg_accounts__cleaned.sql
│   │   │
│   │   ├── intermediate/
│   │   │   ├── int_accounts__with_customer.sql
│   │   │   └── int_savings_accounts_only.sql
│   │   │
│   │   └── marts/
│   │       └── account_summary.sql
│   │
│   ├── tests/
│   │   ├── generic/
│   │   │   └── (custom generic tests)
│   │   │
│   │   └── unit_tests/
│   │       ├── test_stg_customers.dml
│   │       └── test_account_summary.sql
│   │
│   ├── macros/
│   │   └── calculate_interest_rate.sql
│   │
│   └── seeds/
│       └── (seed data if needed)
│
├── data/
│   ├── inputs/
│   │   ├── customers.csv             # Input data
│   │   ├── accounts.csv
│   │   └── .gitkeep
│   │
│   ├── outputs/
│   │   ├── account_summary.csv
│   │   ├── account_summary.parquet
│   │   └── .gitkeep
│   │
│   └── duckdb/
│       └── analytics.duckdb          # Local database (gitignore)
│
├── tests/
│   ├── conftest.py                   # Pytest fixtures
│   ├── test_assets.py                # Asset execution tests
│   ├── test_definitions.py           # Definitions validation
│   └── test_data_quality.py          # Quality checks
│
├── .github/
│   └── workflows/
│       └── ci.yml                    # GitHub Actions for testing
│
└── docs/
    ├── ARCHITECTURE.md               # This design document
    ├── DESIGN_DECISIONS.md           # Why each choice
    ├── SCALING_GUIDE.md              # How to scale to production
    └── TROUBLESHOOTING.md            # Common issues
```

**Why this structure?**

1. **Separation by responsibility** - Assets, resources, IO managers clearly separated
2. **Colocated DBT** - Single repository contains all data platform code
3. **Clear inputs/outputs** - `data/` folder shows what enters and exits
4. **Test cohabitation** - Tests next to code for easy discovery
5. **Scalable to team** - Adding team members: assign them specific `models/` subdirectories
6. **Production-ready** - Same structure works for enterprise deployments

**Decision: Monorepo with Dagster + DBT colocated, organized by responsibility**

---

## Part 4: Data Flow and Asset Design

### Asset Graph (Dagster)

```python
# Conceptual asset graph:

customers_raw (Source)
    ↓
@asset customers_cleaned (via ingestion.py)
    ↓
@dbt_assets stg_customers__cleaned
    ↓
    └──→ int_accounts__with_customer
    
accounts_raw (Source)
    ↓
@asset accounts_cleaned (via ingestion.py)
    ↓
@dbt_assets stg_accounts__cleaned
    ↓
    └──→ int_savings_accounts_only
            ↓
            └──→ account_summary (marts)
                    ↓
                @asset account_summary_exported (to CSV/Parquet)
                    ↓
                @asset account_summary_to_databricks (production only)
```

### Asset Definitions

#### 1. Ingestion Assets (Read CSVs)

```python
# src/lending_club_pipeline/assets/ingestion.py

@asset(
    name="customers_raw",
    group_name="ingestion",
    owners=["team:data-eng"],
    tags={"domain": "customer", "layer": "raw"},
    metadata={
        "owner": "Data Engineering",
        "description": "Raw customer data from source CSV",
        "freshness": {"warn_after": {"minutes": 60}},
    }
)
def customers_raw() -> pd.DataFrame:
    """Read and validate customers.csv from input data directory."""
    df = pd.read_csv("data/inputs/customers.csv")
    # Basic validation
    assert not df.empty, "Customers CSV is empty"
    return df

@asset(
    name="accounts_raw",
    group_name="ingestion",
    owners=["team:data-eng"],
    tags={"domain": "account", "layer": "raw"},
)
def accounts_raw() -> pd.DataFrame:
    """Read and validate accounts.csv from input data directory."""
    df = pd.read_csv("data/inputs/accounts.csv")
    assert not df.empty, "Accounts CSV is empty"
    return df
```

**Why this design?**
- ✓ Source truth is version-controlled CSVs (in `data/inputs/`)
- ✓ Dagster tracks when files were last read (lineage)
- ✓ Basic validation happens at ingestion boundary
- ✓ Staging layer doesn't repeat source reading

#### 2. DBT Assets (Transformations)

```python
# src/lending_club_pipeline/assets/dbt_assets.py

from dagster_dbt import DbtProject, dbt_assets

dbt_project = DbtProject(project_dir="dbt_project")

@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="staging,intermediate,marts"  # Run all transformation layers
)
def dbt_transformations(context: AssetExecutionContext, dbt: DbtCliResource):
    """Execute all DBT models through the transformation layers."""
    yield from dbt.cli(["build"], context=context).stream()
```

**Why DBT assets?**
- ✓ Automatic lineage tracking from `ref()` calls in DBT
- ✓ Granular asset checks (each model becomes a Dagster asset)
- ✓ DBT test failures surface as asset check failures
- ✓ Production-ready pattern (scales to 100+ models)

#### 3. Output Assets (Export Results)

```python
# src/lending_club_pipeline/assets/outputs.py

@asset(
    name="account_summary_csv",
    group_name="outputs",
    io_manager_key="csv_io_manager",
    depends_on={"account_summary": AssetIn(key_prefix=["dbt"])},
)
def account_summary_csv(context: AssetExecutionContext) -> pd.DataFrame:
    """Export account summary to CSV for inspection."""
    # Load from DBT-materialized table, convert to CSV
    # CSV written to data/outputs/account_summary.csv
    pass

@asset(
    name="account_summary_to_databricks",
    group_name="outputs",
    resource_defs={"databricks": databricks_resource},
)
def account_summary_to_databricks(context: AssetExecutionContext):
    """Load account summary to Databricks (production only)."""
    # Only runs if DATABRICKS_HOST is configured
    # Reads from local table, writes to Databricks Delta table
    pass
```

**Why separate output assets?**
- ✓ Allows different output formats without DBT changes
- ✓ CSV export runs locally, Databricks load is production-only
- ✓ Clear separation: DBT owns data quality, output assets own format
- ✓ Reusable (multiple reports can source from `account_summary`)

**Decision: Three-tier asset structure (ingestion → dbt transformations → outputs)**

---

## Part 5: Configuration and Environment Management

### Configuration Strategy: Environment-Driven

**Why this matters:** Same code, different environments (dev/staging/prod) with zero changes to Python or SQL.

#### Local Development (`dev` environment)

```yaml
# .env.dev
ENVIRONMENT=dev
DAGSTER_HOME=./dagster_home
DATABASE_TYPE=duckdb
DUCKDB_PATH=./data/duckdb/analytics.duckdb
DBT_TARGET=dev
DBT_SCHEMA=analytics_dev
OUTPUT_PATH=./data/outputs
```

#### Production (`prod` environment)

```yaml
# .env.prod (stored in Kubernetes secrets)
ENVIRONMENT=prod
DATABASE_TYPE=databricks
DATABRICKS_HOST=https://xxxxx.databricks.com
DATABRICKS_TOKEN=${DATABRICKS_TOKEN}  # From secret manager
DATABRICKS_CATALOG=prod
DATABRICKS_SCHEMA=analytics
DBT_TARGET=prod
DBT_SCHEMA=analytics
OUTPUT_PATH=s3://bucket-name/outputs
```

#### Python Code to Load Configuration

```python
# src/lending_club_pipeline/resources/config.py

from dataclasses import dataclass
import os
from typing import Literal

@dataclass
class DataPlatformConfig:
    """Centralized configuration loaded from environment variables."""
    
    environment: Literal["dev", "staging", "prod"]
    database_type: Literal["duckdb", "databricks"]
    output_path: str
    
    # DuckDB specific
    duckdb_path: str = None
    
    # Databricks specific
    databricks_host: str = None
    databricks_token: str = None
    databricks_catalog: str = None
    databricks_schema: str = None
    
    @classmethod
    def from_env(cls) -> "DataPlatformConfig":
        """Load configuration from environment variables."""
        return cls(
            environment=os.getenv("ENVIRONMENT", "dev"),
            database_type=os.getenv("DATABASE_TYPE", "duckdb"),
            output_path=os.getenv("OUTPUT_PATH", "./data/outputs"),
            duckdb_path=os.getenv("DUCKDB_PATH", "./data/duckdb/analytics.duckdb"),
            databricks_host=os.getenv("DATABRICKS_HOST"),
            databricks_token=os.getenv("DATABRICKS_TOKEN"),
            databricks_catalog=os.getenv("DATABRICKS_CATALOG"),
            databricks_schema=os.getenv("DATABRICKS_SCHEMA"),
        )

# Usage in assets
config = DataPlatformConfig.from_env()

if config.database_type == "databricks":
    # Production logic using Databricks
    pass
elif config.database_type == "duckdb":
    # Local logic using DuckDB
    pass
```

**Why this pattern?**
- ✓ Single source of configuration (environment variables)
- ✓ Secrets never in code (externalized to Kubernetes/Docker)
- ✓ Type-safe configuration (Pydantic or dataclasses)
- ✓ Easy testing (mock config for tests)
- ✓ Production-ready (Kubernetes secrets inject into env)

#### DBT Profile Configuration

```yaml
# dbt_project/profiles.yml

lending_club:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH') }}"
      schema: "{{ env_var('DBT_SCHEMA', 'analytics_dev') }}"
      threads: 4
      
    prod:
      type: databricks
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      catalog: "{{ env_var('DATABRICKS_CATALOG', 'prod') }}"
      schema: "{{ env_var('DBT_SCHEMA', 'analytics') }}"
      threads: 8
```

**Why this DBT setup?**
- ✓ No hardcoded credentials
- ✓ Profile auto-switches based on `DBT_TARGET` variable
- ✓ Thread count differs per environment (dev: 4, prod: 8)
- ✓ Same `dbt_project.yml` works everywhere

**Decision: Externalized environment-driven configuration via `.env` files**

---

## Part 6: Data Quality and Testing Strategy

### Testing Pyramid

```
        △ End-to-End Tests
       ╱ ╲  (Full pipeline on sample data)
      ╱   ╲
     ╱─────╲ Integration Tests
    ╱       ╲ (DBT models + Dagster assets)
   ╱─────────╲ Unit Tests
  ╱ (Asset logic, DBT models)
 △───────────▽
```

### Layer 1: Unit Tests (DBT Models)

```sql
-- dbt_project/tests/unit_tests/test_stg_customers.sql
-- Test that stg_customers cleans data correctly

select *
from {{ ref('stg_customers__cleaned') }}
where customer_id is null
or name = ''
or has_loan not in ('Yes', 'No', 'None')
```

```sql
-- dbt_project/tests/unit_tests/test_interest_calculation.sql
-- Validate interest rate calculation logic

select *
from {{ ref('account_summary') }}
where interest_rate < 0.01 or interest_rate > 0.03
  and account_type = 'Savings'
```

**DBT Generic Tests (auto-applied):**

```yaml
# dbt_project/models/staging/stg_customers__cleaned.yml

models:
  - name: stg_customers__cleaned
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: name
        tests:
          - not_null
      - name: has_loan
        tests:
          - accepted_values:
              values: ['Yes', 'No', 'None']

  - name: stg_accounts__cleaned
    columns:
      - name: account_id
        tests:
          - unique
          - not_null
      - name: balance
        tests:
          - not_null
      - name: account_type
        tests:
          - accepted_values:
              values: ['Savings', 'Checking']
```

### Layer 2: Integration Tests (Dagster Assets)

```python
# tests/test_assets.py

from dagster import build_op_context, execute_job

def test_customers_raw_asset():
    """Test that ingestion asset reads CSV correctly."""
    from src.lending_club_pipeline.assets.ingestion import customers_raw
    
    df = customers_raw()
    
    assert not df.empty, "Customers CSV should not be empty"
    assert "CustomerID" in df.columns
    assert len(df) > 0

def test_dbt_transformation_integration():
    """Test full DBT transformation pipeline."""
    from dagster_dbt import execute_dbt_job
    
    result = execute_dbt_job(
        dbt_project_dir="dbt_project",
        args=["test"],  # Run all DBT tests
    )
    
    assert result.success, "DBT tests should pass"

def test_data_quality_checks():
    """Test custom data quality conditions."""
    import pandas as pd
    from src.lending_club_pipeline.utils.data_validation import validate_account_summary
    
    # Load generated account_summary
    df = pd.read_sql(
        "SELECT * FROM account_summary",
        conn=duckdb.connect("data/duckdb/analytics.duckdb")
    )
    
    # Run custom validations
    issues = validate_account_summary(df)
    
    assert not issues, f"Data quality checks failed: {issues}"
```

### Layer 3: End-to-End Tests

```python
# tests/test_full_pipeline.py

def test_full_pipeline_execution():
    """Test complete pipeline from raw data to output."""
    from dagster import materialize
    from src.lending_club_pipeline.definitions import defs
    
    # Materialize all assets
    result = materialize(
        [defs.get_asset_graph()],
        raise_on_error=True,
    )
    
    assert result.success
    
    # Verify outputs exist
    assert os.path.exists("data/outputs/account_summary.csv")
    
    # Verify output data quality
    df = pd.read_csv("data/outputs/account_summary.csv")
    assert len(df) > 0
    assert all(col in df.columns for col in [
        'customer_id', 'account_id', 'original_balance',
        'interest_rate', 'annual_interest', 'new_balance'
    ])
```

**Decision: Implement three-tier testing (DBT tests → Dagster integration tests → end-to-end tests)**

---

## Part 7: Scalability Patterns (Demo → Production)

### Partitioning Strategy (Future-Proof)

Even for demo with small datasets, implement partitioning to show production thinking:

```python
# src/lending_club_pipeline/assets/partitioned_assets.py

from dagster import DailyPartitionsDefinition, asset

daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

@asset(
    partitions_def=daily_partitions,
    group_name="ingestion",
)
def customers_daily(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Partitioned customer ingestion by date.
    
    Demo: Loads same customer data for each date.
    Production: Loads daily customer changes from data lake.
    """
    date_partition = context.partition_time_window.start
    
    # Demo: Static file for all dates
    if os.getenv("ENVIRONMENT") == "dev":
        return pd.read_csv("data/inputs/customers.csv")
    
    # Production: Read date-specific partition from Databricks
    return read_from_databricks(
        table="raw.customers_daily",
        date=date_partition,
    )
```

**Why partition early?**
- ✓ Shows understanding of scalable architecture
- ✓ Same code handles 100 records (1 partition) or 100M records (365 partitions)
- ✓ Enables backfilling if data is wrong
- ✓ Demonstrates production readiness to interviewers

### Incremental Model Pattern (DBT)

```sql
-- dbt_project/models/marts/account_summary.sql

{{
  config(
    materialized='incremental',
    unique_key='account_id',
    on_schema_change='sync_all_columns',
  )
}}

with customers as (
    select * from {{ ref('stg_customers__cleaned') }}
    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

accounts as (
    select * from {{ ref('int_savings_accounts_only') }}
    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select
    customers.customer_id,
    accounts.account_id,
    accounts.balance as original_balance,
    case
        when accounts.balance < 10000 then 0.01
        when accounts.balance < 20000 then 0.015
        else 0.02
    end +
    case when customers.has_loan = 'Yes' then 0.005 else 0 end as interest_rate,
    
    round(accounts.balance * (
        case
            when accounts.balance < 10000 then 0.01
            when accounts.balance < 20000 then 0.015
            else 0.02
        end +
        case when customers.has_loan = 'Yes' then 0.005 else 0 end
    ), 2) as annual_interest,
    
    round(accounts.balance + round(accounts.balance * (
        case
            when accounts.balance < 10000 then 0.01
            when accounts.balance < 20000 then 0.015
            else 0.02
        end +
        case when customers.has_loan = 'Yes' then 0.005 else 0 end
    ), 2), 2) as new_balance

from accounts
join customers using (customer_id)
```

**Why incremental models?**
- ✓ First full load processes all data
- ✓ Subsequent runs process only changed records
- ✓ Scales from 1K to 1B rows without code changes
- ✓ Shows optimization thinking in interview

**Decision: Implement partitioning and incremental models even for demo data**

---

## Part 8: Interview Demo Roadmap

### What You'll Demo (5-10 minutes)

1. **Show repository structure** (30 seconds)
   - "Here's the monorepo with Dagster + DBT colocated"
   - Point out separation: `assets/`, `dbt_project/`, `tests/`

2. **Run local stack** (1 minute)
   ```bash
   docker-compose up
   # Shows Dagster webserver starting, DuckDB connecting
   ```

3. **Open Dagster UI** (2 minutes)
   - Navigate to `http://localhost:3000`
   - Show asset lineage graph (raw → staging → intermediate → marts)
   - Click on asset, show lineage details

4. **Trigger pipeline** (2 minutes)
   ```bash
   dagster asset materialize
   # Or use UI to materialize all assets
   ```
   - Show execution logs in real-time
   - Show DBT model tests passing
   - Show output files generated

5. **Inspect results** (1 minute)
   ```bash
   head data/outputs/account_summary.csv
   # Show account_id, customer_id, balance, interest_rate, new_balance
   ```

6. **Walk through code** (3 minutes)
   - Open `src/lending_club_pipeline/assets/ingestion.py` - explain ingestion asset
   - Open `dbt_project/models/marts/account_summary.sql` - explain interest calculation
   - Open `tests/test_assets.py` - explain testing strategy

7. **Discuss scalability** (2 minutes)
   - Explain configuration-driven design: same code runs on DuckDB or Databricks
   - Point to `.env` file showing environment switching
   - Discuss partitioning strategy for millions of records
   - Mention Kubernetes deployment for production

### What You'll Show in Slides

**Slide 1: Architecture Overview**
- Data flow diagram (sources → staging → intermediate → marts)
- Technology choices and why

**Slide 2: Design Decisions**
- Why Dagster (asset-centric, DBT integration)
- Why DBT (transformation layer, testability)
- Why three-layer architecture (quality gates, reusability)

**Slide 3: Key Features**
- Asset lineage tracking
- Comprehensive testing (DBT tests + Dagster checks)
- Environment-driven configuration
- Partition-ready for scaling

**Slide 4: Production Readiness**
- Error handling and retries
- Data quality checks at each layer
- Monitoring and alerting setup
- Path to scale: DuckDB → Databricks

**Slide 5: Lessons Learned / Trade-offs**
- What worked well
- What was challenging
- If you had more time, what you'd improve

---

## Part 9: Why Each Component Choice

### Summary Decision Matrix

| Component | Choice | Why | Alternative | Why Not |
|-----------|--------|-----|-------------|---------|
| **Orchestration** | Dagster | Asset-centric, DBT integration, local dev | Airflow | Complex, older patterns |
| **Transformation** | DBT | SQL-based, testable, 3-layer pattern | Spark | Overkill, harder to test |
| **Local Storage** | DuckDB | OLAP-optimized, CSV-native, fast | SQLite | OLTP-focused |
| **Production Storage** | Databricks | Delta Lake, Unity Catalog, ML integration | Snowflake | Works, but limited to Snowflake |
| **Container Orchestration** | Docker Compose | Local dev, mirrors production | Kubernetes | Overkill for local |
| **Language** | Python + SQL | Dagster native, DBT native, flexible | Python only | Missing SQL best practices |
| **Project Structure** | Monorepo | Single source of truth, team scaling | Multi-repo | Version mismatch issues |
| **Testing** | Three-tier | Comprehensive coverage, pyramid shape | Single layer | Insufficient coverage |
| **Configuration** | Environment-driven | No code changes between envs | Hard-coded | Risk of mistakes |

### Decision Principles Applied

1. **Separation of Concerns** ✓
   - Dagster handles orchestration (when/how)
   - DBT handles transformation (what logic)
   - Environment config handles infrastructure (where)

2. **Production-Ready from Day One** ✓
   - Partitioning support built-in
   - Incremental models in place
   - Three-layer quality gates
   - Comprehensive testing

3. **Scalability Without Refactoring** ✓
   - Same code runs on 1KB or 1TB of data
   - Only configuration changes (env vars)
   - Partitioned assets automatically parallelize
   - Resource limits configurable per asset

4. **Interview Impression** ✓
   - Demonstrates modern best practices
   - Shows understanding of trade-offs
   - Exhibits production thinking
   - Professional code organization

---

## Part 10: Implementation Checklist

### Phase 1: Foundation (Hours 1-2)
- [ ] Initialize Dagster project with `dagster project scaffold`
- [ ] Set up DBT project with `dbt init`
- [ ] Create Docker Compose with Dagster + PostgreSQL
- [ ] Set up `.env.example` and `config.py`

### Phase 2: Ingestion (Hours 2-3)
- [ ] Create ingestion assets reading `customers.csv` and `accounts.csv`
- [ ] Add basic validation (not null, not empty)
- [ ] Write unit tests for ingestion

### Phase 3: DBT Transformations (Hours 3-4)
- [ ] Create staging layer (`stg_customers`, `stg_accounts`)
- [ ] Create intermediate layer (join, filter)
- [ ] Create marts layer (`account_summary`)
- [ ] Integrate DBT with Dagster via `@dbt_assets`

### Phase 4: Data Quality (Hours 4-5)
- [ ] Add DBT generic tests (unique, not_null, accepted_values)
- [ ] Add custom data quality checks
- [ ] Create asset checks in Dagster

### Phase 5: Outputs (Hours 5-6)
- [ ] Export to CSV
- [ ] Export to Parquet
- [ ] Add Databricks loading asset (conditional)

### Phase 6: Testing & Documentation (Hours 6-7)
- [ ] Write integration tests
- [ ] Create README with setup instructions
- [ ] Document design decisions
- [ ] Add architecture diagram

### Phase 7: Polish & Demo (Hours 7-8)
- [ ] Test Docker Compose locally
- [ ] Create demo script
- [ ] Prepare slides
- [ ] Practice demo walkthrough

---

## Conclusion

This design applies production-grade patterns while remaining demo-friendly:

✓ **Demonstrates understanding** of modern data architecture
✓ **Production-capable** from day one (no missing patterns)
✓ **Scalable** from sample data to billions of records
✓ **Testable** at multiple levels
✓ **Maintainable** with clear structure and responsibilities
✓ **Interview-impressive** showing sophisticated design thinking

The key insight: **Start with the right foundation. The patterns scale naturally without refactoring.**

---

## References

**Official Documentation:**
- Dagster: https://docs.dagster.io
- DBT: https://docs.getdbt.com
- DuckDB: https://duckdb.org/docs
- Databricks: https://docs.databricks.com

**Production Examples:**
- Dagster hooli-data-eng-pipelines: https://github.com/dagster-io/hooli-data-eng-pipelines
- Dagster open-platform: https://github.com/dagster-io/dagster/tree/master/examples/dagster-open-platform

**Best Practices:**
- Medallion Architecture: https://www.databricks.com/blog/2022/06/24/multi-hop-architecture-modularity-by-default.html
- DBT Best Practices: https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview
- Data Quality: https://docs.getdbt.com/docs/build/data-tests
