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

{{ config(materialized='ephemeral') }}

with accounts_with_customer as (
    select
        account_id,
        customer_id,
        balance,
        account_type,
        has_loan
    from {{ ref('int_accounts__with_customer') }}
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
