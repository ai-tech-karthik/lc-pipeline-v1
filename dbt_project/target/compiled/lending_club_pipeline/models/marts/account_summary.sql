/*
    Marts model: Account Summary with Interest Calculation
    
    Purpose: Generate final account summary with calculated interest rates
    and projected balances based on business rules.
    
    Business Logic:
    - Base interest rate by balance tier:
        * < $10,000: 1.0% (0.01)
        * $10,000 - $19,999: 1.5% (0.015)
        * >= $20,000: 2.0% (0.02)
    - Bonus rate: +0.5% (0.005) if customer has_loan = 'Yes'
    - Annual interest: balance * interest_rate, rounded to 2 decimals
    - New balance: original_balance + annual_interest, rounded to 2 decimals
    
    Materialization: table (persistent storage for query performance)
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
),  __dbt__cte__int_savings_accounts_only as (
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



with accounts_with_customer as (
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
), savings_accounts as (
    select
        customer_id,
        account_id,
        balance,
        has_loan
    from __dbt__cte__int_savings_accounts_only
),

interest_calculation as (
    select
        customer_id,
        account_id,
        balance as original_balance,
        
        -- Calculate base interest rate based on balance tiers
        case
            when balance < 10000 then 0.01
            when balance >= 10000 and balance < 20000 then 0.015
            when balance >= 20000 then 0.02
        end as base_rate,
        
        -- Add bonus rate if customer has a loan
        case
            when has_loan = 'Yes' then 0.005
            else 0.0
        end as bonus_rate
        
    from savings_accounts
),

final_calculation as (
    select
        customer_id,
        account_id,
        original_balance,
        
        -- Total interest rate (base + bonus)
        base_rate + bonus_rate as interest_rate,
        
        -- Calculate annual interest (rounded to 2 decimals)
        round(original_balance * (base_rate + bonus_rate), 2) as annual_interest
        
    from interest_calculation
)

select
    customer_id,
    account_id,
    original_balance,
    interest_rate,
    annual_interest,
    
    -- Calculate new balance (rounded to 2 decimals)
    round(original_balance + annual_interest, 2) as new_balance
    
from final_calculation