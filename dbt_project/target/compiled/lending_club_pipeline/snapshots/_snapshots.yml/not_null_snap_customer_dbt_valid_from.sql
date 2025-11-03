
    
    



select dbt_valid_from
from workspace.snapshots.snap_customer
where dbt_valid_from is null


