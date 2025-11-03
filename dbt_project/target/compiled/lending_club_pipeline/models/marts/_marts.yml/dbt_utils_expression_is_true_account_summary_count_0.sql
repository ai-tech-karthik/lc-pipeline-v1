



select
    1
from workspace.default_marts.account_summary

where not(count(*) > 0)

