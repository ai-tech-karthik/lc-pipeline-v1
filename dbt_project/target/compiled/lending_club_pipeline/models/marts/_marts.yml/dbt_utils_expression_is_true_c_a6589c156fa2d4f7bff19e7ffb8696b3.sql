



select
    1
from workspace.default_marts.customer_profile

where not(total_accounts_count > 0)

