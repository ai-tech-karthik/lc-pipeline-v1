"""
Ingestion assets for reading and validating raw CSV data.
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import pandas as pd
from dagster import (
    AssetExecutionContext, 
    asset, 
    Output, 
    MetadataValue,
    AutomationCondition,
)


def validate_csv_data(df: pd.DataFrame, filepath: str, required_columns: List[str]) -> None:
    """
    Validate CSV data for common issues.
    
    Args:
        df: DataFrame to validate
        filepath: Path to the CSV file (for error messages)
        required_columns: List of column names that must be present
        
    Raises:
        ValueError: If validation fails
    """
    # Check for empty file
    if df.empty:
        raise ValueError(f"CSV file is empty: {filepath}")
    
    # Check for missing columns
    actual_columns = set(df.columns)
    expected_columns = set(required_columns)
    missing_columns = expected_columns - actual_columns
    
    if missing_columns:
        raise ValueError(
            f"Missing required columns in {filepath}: {', '.join(sorted(missing_columns))}"
        )


@asset(
    group_name="ingestion",
    tags={
        "layer": "raw", 
        "source": "csv",
        "domain": "customer",
        "sla": "daily",
        "priority": "high",
    },
    description="Raw customer data ingested from Customer.csv with validation. This asset reads customer information including ID, name, and loan status from the source CSV file and performs data quality checks.",
    metadata={
        "schema": "raw",
        "owner": "data-engineering-team",
        "source_system": "lending_club_csv",
        "data_classification": "internal",
        "refresh_frequency": "daily",
        "expected_lag_hours": 24,
        "business_purpose": "Provide customer master data for account analysis",
        "data_quality_checks": "empty_file, missing_columns, column_name_validation",
        "downstream_consumers": "staging layer (stg_customers__cleaned)",
        "freshness_sla": "24 hours - Expected to refresh daily at 2 AM",
    },
    owners=["data-engineering-team@company.com"],
)
def customers_raw(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """
    Read and validate customer data from CSV file.
    
    Returns:
        Output containing validated customer DataFrame with metadata
        
    Raises:
        FileNotFoundError: If the CSV file does not exist
        ValueError: If the file is empty or missing required columns
    """
    # Define file path and required columns
    filepath = Path("data/inputs/Customer.csv")
    required_columns = ["CustomerID", "Name", "HasLoan"]
    
    try:
        # Check if file exists
        if not filepath.exists():
            context.log.error(f"File not found: {filepath}")
            raise FileNotFoundError(f"Input file not found: {filepath}")
        
        # Read CSV file
        context.log.info(f"Reading customer data from {filepath}")
        
        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            context.log.error(f"Failed to read CSV file {filepath}: {type(e).__name__} - {str(e)}")
            raise
        
        # Strip whitespace from column names
        df.columns = df.columns.str.strip()
        
        # Validate data
        try:
            validate_csv_data(df, str(filepath), required_columns)
        except ValueError as e:
            context.log.error(f"Validation failed for {filepath}: {str(e)}")
            raise
        
        # Log success
        context.log.info(f"Successfully loaded {len(df)} customer records")
        
        # Return with metadata
        return Output(
            value=df,
            metadata={
                "row_count": len(df),
                "column_names": MetadataValue.text(", ".join(df.columns.tolist())),
                "execution_timestamp": datetime.now().isoformat(),
                "file_path": str(filepath),
                "preview": MetadataValue.md(df.head().to_markdown()),
            }
        )
    
    except FileNotFoundError:
        # Re-raise FileNotFoundError with context already logged
        raise
    except ValueError:
        # Re-raise ValueError with context already logged
        raise
    except Exception as e:
        # Catch any unexpected errors
        context.log.error(f"Unexpected error processing {filepath}: {type(e).__name__} - {str(e)}")
        raise


@asset(
    group_name="ingestion",
    tags={
        "layer": "raw", 
        "source": "csv",
        "domain": "account",
        "sla": "daily",
        "priority": "high",
    },
    description="Raw account data ingested from accounts.csv with validation. This asset reads account information including account ID, customer ID, balance, and account type from the source CSV file and performs data quality checks.",
    metadata={
        "schema": "raw",
        "owner": "data-engineering-team",
        "source_system": "lending_club_csv",
        "data_classification": "internal",
        "refresh_frequency": "daily",
        "expected_lag_hours": 24,
        "business_purpose": "Provide account transaction data for interest calculation",
        "data_quality_checks": "empty_file, missing_columns, column_name_validation",
        "downstream_consumers": "staging layer (stg_accounts__cleaned)",
        "freshness_sla": "24 hours - Expected to refresh daily at 2 AM",
    },
    owners=["data-engineering-team@company.com"],
    deps=[customers_raw],  # Ensure sequential execution to avoid DuckDB lock conflicts
)
def accounts_raw(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """
    Read and validate account data from CSV file.
    
    Returns:
        Output containing validated account DataFrame with metadata
        
    Raises:
        FileNotFoundError: If the CSV file does not exist
        ValueError: If the file is empty or missing required columns
    """
    # Define file path and required columns
    filepath = Path("data/inputs/accounts.csv")
    required_columns = ["AccountID", "CustomerID", "Balance", "AccountType"]
    
    try:
        # Check if file exists
        if not filepath.exists():
            context.log.error(f"File not found: {filepath}")
            raise FileNotFoundError(f"Input file not found: {filepath}")
        
        # Read CSV file
        context.log.info(f"Reading account data from {filepath}")
        
        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            context.log.error(f"Failed to read CSV file {filepath}: {type(e).__name__} - {str(e)}")
            raise
        
        # Strip whitespace from column names
        df.columns = df.columns.str.strip()
        
        # Validate data
        try:
            validate_csv_data(df, str(filepath), required_columns)
        except ValueError as e:
            context.log.error(f"Validation failed for {filepath}: {str(e)}")
            raise
        
        # Log success
        context.log.info(f"Successfully loaded {len(df)} account records")
        
        # Return with metadata
        return Output(
            value=df,
            metadata={
                "row_count": len(df),
                "column_names": MetadataValue.text(", ".join(df.columns.tolist())),
                "execution_timestamp": datetime.now().isoformat(),
                "file_path": str(filepath),
                "preview": MetadataValue.md(df.head().to_markdown()),
            }
        )
    
    except FileNotFoundError:
        # Re-raise FileNotFoundError with context already logged
        raise
    except ValueError:
        # Re-raise ValueError with context already logged
        raise
    except Exception as e:
        # Catch any unexpected errors
        context.log.error(f"Unexpected error processing {filepath}: {type(e).__name__} - {str(e)}")
        raise
