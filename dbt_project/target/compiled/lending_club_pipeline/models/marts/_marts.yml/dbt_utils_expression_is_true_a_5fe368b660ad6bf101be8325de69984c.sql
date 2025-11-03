



select
    1
from workspace.default_marts.account_summary

where not(original_balance_amount >= 0)

