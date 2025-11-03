



select
    1
from workspace.default_marts.account_summary

where not(annual_interest_amount >= 0)

