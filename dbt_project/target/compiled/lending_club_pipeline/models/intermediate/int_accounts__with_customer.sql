/*
    Intermediate model: Accounts with Customer Information
    
    Purpose: Join account records with customer information to enable
    customer-context analysis of accounts.
    
    Business Logic:
    - INNER JOIN ensures only accounts with valid customer records are included
    - Preserves all account fields for downstream analysis
    - Adds has_loan attribute from customer for interest rate calculations
    
    Materialization: ephemeral (computed on-the-fly, not stored)
*/



with accounts as (
    select
        account_id,
        customer_id,
        balance,
        account_type
    from lending_club.main_staging.stg_accounts__cleaned
),

customers as (
    select
        customer_id,
        name,
        has_loan
    from lending_club.main_staging.stg_customers__cleaned
),

joined as (
    select
        -- All account fields
        accounts.account_id,
        accounts.customer_id,
        accounts.balance,
        accounts.account_type,
        
        -- Customer attribute needed for downstream calculations
        customers.has_loan
        
    from accounts
    inner join customers
        on accounts.customer_id = customers.customer_id
)

select * from joined