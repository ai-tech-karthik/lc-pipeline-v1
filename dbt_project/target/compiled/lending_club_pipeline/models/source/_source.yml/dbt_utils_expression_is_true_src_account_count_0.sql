



select
    1
from workspace.default_source.src_account

where not(count(*) > 0)

