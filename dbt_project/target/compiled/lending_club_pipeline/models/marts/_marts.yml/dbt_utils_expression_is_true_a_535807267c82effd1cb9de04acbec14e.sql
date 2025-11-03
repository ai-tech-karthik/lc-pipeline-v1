



select
    1
from workspace.default_marts.account_summary

where not(interest_rate_pct >= 0.01 and <= 0.025)

