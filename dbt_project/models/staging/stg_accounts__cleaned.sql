/*
    Staging model for account data
    
    Purpose: Clean and normalize raw account data from accounts.csv
    
    Transformations:
    - Trim whitespace from string fields (AccountID, AccountType)
    - Cast CustomerID to integer and Balance to decimal(10,2)
    - Normalize AccountType to proper case ('Savings', 'Checking')
    - Filter out records where Balance is null or empty
    
    Materialization: view (configured in dbt_project.yml)
*/

with source as (
    select
        AccountID,
        CustomerID,
        Balance,
        AccountType
    from {{ source('raw', 'accounts_raw') }}
),

cleaned as (
    select
        -- Trim whitespace from AccountID (string field)
        trim(cast(AccountID as string)) as account_id,
        
        -- Cast CustomerID to integer (already numeric from CSV ingestion)
        cast(CustomerID as integer) as customer_id,
        
        -- Cast Balance to decimal(10,2) (already numeric from CSV ingestion)
        cast(Balance as decimal(10,2)) as balance,
        
        -- Normalize AccountType to proper case ('Savings', 'Checking')
        case
            when lower(trim(cast(AccountType as string))) = 'savings' then 'Savings'
            when lower(trim(cast(AccountType as string))) = 'checking' then 'Checking'
            else trim(cast(AccountType as string))  -- Preserve original if not recognized
        end as account_type
        
    from source
    
    -- Filter out records where Balance is null or empty
    where Balance is not null
)

select * from cleaned
