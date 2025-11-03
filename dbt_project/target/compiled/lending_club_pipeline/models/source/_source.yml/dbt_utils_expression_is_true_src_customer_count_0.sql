



select
    1
from workspace.default_source.src_customer

where not(count(*) > 0)

