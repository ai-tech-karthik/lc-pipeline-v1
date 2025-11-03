



select
    1
from workspace.default_marts.customer_profile

where not(count(*) > 0)

