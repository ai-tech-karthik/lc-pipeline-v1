



select
    1
from workspace.default_marts.account_summary

where not(new_balance_amount > original_balance_amount)

