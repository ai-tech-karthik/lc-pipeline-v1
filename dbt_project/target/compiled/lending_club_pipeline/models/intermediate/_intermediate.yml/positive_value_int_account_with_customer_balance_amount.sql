

-- Test that a numeric column contains only positive values (> 0)
-- This is useful for validating amounts, balances, counts, etc.

select
    balance_amount as value,
    count(*) as violation_count
from workspace.default_intermediate.int_account_with_customer
where balance_amount <= 0
   or balance_amount is null
group by balance_amount

