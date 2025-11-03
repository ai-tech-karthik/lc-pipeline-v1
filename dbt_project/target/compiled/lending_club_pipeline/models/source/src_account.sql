/*
    Source model for account data
    
    Purpose: Persist raw account data without transformation.
    This is the first layer in the five-layer architecture, capturing data 
    exactly as received from CSV files.
    
    Layer: Source (Layer 1)
    Materialization: table (configured in dbt_project.yml)
    Contract: Enforced (defined in _source.yml)
    
    Transformations:
    - Add loaded_at timestamp to track when data was ingested
    - No other transformations - preserve raw data exactly as received
*/

select
    AccountID as account_id,
    CustomerID as customer_id,
    Balance as balance,
    AccountType as account_type,
    current_timestamp() as loaded_at
from `workspace`.`raw`.`accounts_raw`