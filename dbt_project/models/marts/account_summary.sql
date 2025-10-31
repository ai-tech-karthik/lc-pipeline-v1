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

{{ config(materialized='table') }}

with savings_accounts as (
    select
        customer_id,
        account_id,
        balance,
        has_loan
    from {{ ref('int_savings_accounts_only') }}
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
