/*
    Intermediate model: Savings Accounts Only
    
    Purpose: Filter accounts to include only savings accounts for interest
    rate calculations and analysis.
    
    Business Logic:
    - Filters to account_type = 'Savings' only
    - Excludes checking accounts from downstream processing
    - Preserves all fields from the joined account-customer data
    
    Materialization: ephemeral (computed on-the-fly, not stored)
*/



with  __dbt__cte__int_accounts__with_customer as (
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
), accounts_with_customer as (
    select
        account_id,
        customer_id,
        balance,
        account_type,
        has_loan
    from __dbt__cte__int_accounts__with_customer
),

savings_only as (
    select
        account_id,
        customer_id,
        balance,
        has_loan
    from accounts_with_customer
    where account_type = 'Savings'
)

select * from savings_only