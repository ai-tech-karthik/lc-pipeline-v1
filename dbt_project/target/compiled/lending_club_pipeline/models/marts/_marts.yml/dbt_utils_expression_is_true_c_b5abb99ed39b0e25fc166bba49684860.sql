



select
    1
from workspace.default_marts.customer_profile

where not(total_annual_interest_amount >= 0)

