/*
    Staging model for customer data
    
    Purpose: Clean and normalize raw customer data from Customer.csv
    
    Transformations:
    - Trim whitespace from CustomerID and Name fields
    - Cast CustomerID to integer type
    - Normalize Name to lowercase
    - Normalize HasLoan: 'Yes' -> 'Yes', 'No' -> 'No', 'None' -> NULL
    
    Materialization: view (configured in dbt_project.yml)
*/

with source as (
    select
        CustomerID,
        Name,
        HasLoan
    from {{ source('raw', 'customers_raw') }}
),

cleaned as (
    select
        -- CustomerID is already an integer from CSV ingestion
        CustomerID as customer_id,
        
        -- Trim whitespace and normalize Name to lowercase
        lower(trim(Name)) as name,
        
        -- Normalize HasLoan to proper case ('Yes', 'No', NULL for 'None')
        -- Convert string 'None' to actual NULL value
        case
            when lower(trim(HasLoan)) = 'yes' then 'Yes'
            when lower(trim(HasLoan)) = 'no' then 'No'
            when lower(trim(HasLoan)) = 'none' then NULL
            else trim(HasLoan)  -- Preserve original if not recognized
        end as has_loan
        
    from source
)

select * from cleaned
