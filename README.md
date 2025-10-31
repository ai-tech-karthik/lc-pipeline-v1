# Lending Club Data Pipeline

A production-grade data pipeline for processing customer and account data with interest calculations, built with Dagster and DBT.

[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/)
[![Dagster](https://img.shields.io/badge/dagster-1.5%2B-orange)](https://dagster.io/)
[![DBT](https://img.shields.io/badge/dbt-1.6%2B-orange)](https://www.getdbt.com/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## 🎯 Overview

This pipeline processes lending club customer and account data, applies interest calculations, and outputs results in multiple formats (CSV, Parquet, Databricks tables). It supports both local development (DuckDB) and production deployment (Databricks).

### Key Features

- ✅ **Multi-Environment Support** - DuckDB for local dev, Databricks for production
- ✅ **Comprehensive Testing** - 21 DBT tests + automated smoke tests
- ✅ **Docker Ready** - Full containerization with Docker Compose
- ✅ **Data Quality** - Built-in validation and error handling
- ✅ **Observability** - Dagster UI for monitoring and lineage tracking
- ✅ **Production Ready** - 100% test pass rate across all environments

## 📊 Pipeline Architecture

```
CSV Files → Ingestion → DBT Transformations → Outputs
                ↓              ↓                  ↓
           Raw Tables    Staging + Marts    CSV/Parquet/DB
```

**Assets:**
- `customers_raw` - Customer data ingestion
- `accounts_raw` - Account data ingestion
- `stg_customers__cleaned` - Cleaned customer data
- `stg_accounts__cleaned` - Cleaned account data
- `account_summary` - Final mart with interest calculations
- `account_summary_csv` - CSV export
- `account_summary_parquet` - Parquet export
- `account_summary_to_databricks` - Databricks table load

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Docker & Docker Compose (for containerized deployment)
- Databricks account (for production deployment)

### Installation

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/lending-club-pipeline.git
cd lending-club-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your configuration
```

### Run Locally (DuckDB)

```bash
# Configure for DuckDB
export DATABASE_TYPE=duckdb
export DBT_TARGET=dev
export DAGSTER_HOME=$(pwd)/dagster_home

# Run pipeline
dagster asset materialize --select '*' -m src.lending_club_pipeline.definitions
```

### Run with Docker

```bash
# Start containers
docker-compose up -d

# Access Dagster UI
open http://localhost:3000

# Click "Materialize all" in the UI
```

## 📁 Project Structure

```
lending-club-pipeline/
├── src/
│   └── lending_club_pipeline/
│       ├── assets/           # Dagster assets
│       ├── io_managers/      # Custom IO managers
│       └── resources/        # Databricks resources
├── dbt_project/
│   ├── models/
│   │   ├── staging/         # Staging models
│   │   └── marts/           # Mart models
│   └── tests/               # DBT tests
├── tests/                   # Python tests
├── data/
│   ├── inputs/             # Input CSV files
│   ├── outputs/            # Generated outputs
│   └── duckdb/             # DuckDB database
├── docs/                   # Documentation
├── docker-compose.yml      # Docker configuration
├── Dockerfile             # Container definition
└── pyproject.toml         # Python dependencies
```

## 🧪 Testing

### Run Smoke Tests

```bash
python tests/smoke_test.py
```

### Run DBT Tests

```bash
cd dbt_project
dbt test --target dev  # or prod
```

### Test Results

- **DuckDB:** 3/3 smoke tests passed (~20s)
- **Databricks:** 4/4 smoke tests passed (~95s)
- **DBT:** 21/21 tests passed (100% pass rate)

## 📖 Documentation

Comprehensive documentation is available in the repository:

- **[Quick Start Guide](QUICK_START.md)** - Get started quickly
- **[Pipeline Execution Guide](PIPELINE_EXECUTION_GUIDE.md)** - Detailed execution instructions
- **[Docker Testing Guide](DOCKER_DAGSTER_UI_TESTING_GUIDE.md)** - Docker and UI testing
- **[Databricks Setup](DATABRICKS_SETUP_CHECKLIST.md)** - Databricks configuration
- **[Project Completion Summary](PROJECT_COMPLETION_SUMMARY.md)** - Full project overview

## 🔧 Configuration

### Environment Variables

Key configuration in `.env`:

```properties
# Database Type
DATABASE_TYPE=duckdb  # or databricks

# DBT Target
DBT_TARGET=dev  # or prod

# DuckDB Path (for local)
DUCKDB_PATH=/path/to/lending_club.duckdb

# Databricks (for production)
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token
DATABRICKS_CATALOG=workspace
DATABRICKS_SCHEMA=default
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
```

## 📊 Performance

| Environment | Duration | Use Case |
|-------------|----------|----------|
| **DuckDB** | ~20s | Local development, testing |
| **Databricks** | ~95s | Production, large datasets |
| **Docker + DuckDB** | ~40s | Containerized development |
| **Docker + Databricks** | ~95s | Containerized production |

## 🛠️ Technology Stack

- **Orchestration:** Dagster 1.5.0+
- **Transformation:** DBT 1.6.0+
- **Databases:** DuckDB 0.9.0+ / Databricks
- **Data Processing:** Pandas 2.0.0+, PyArrow 13.0.0+
- **Containerization:** Docker, Docker Compose
- **Storage:** PostgreSQL (metadata), DuckDB/Databricks (data)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Dagster](https://dagster.io/) - Data orchestration platform
- [DBT](https://www.getdbt.com/) - Data transformation tool
- [DuckDB](https://duckdb.org/) - In-process SQL database
- [Databricks](https://databricks.com/) - Cloud data platform

## 📧 Contact

For questions or support, please open an issue in the GitHub repository.

---

**Status:** ✅ Production Ready  
**Last Updated:** October 31, 2025  
**Version:** 1.0.0
