
    
    



select dbt_updated_at
from workspace.snapshots.snap_customer
where dbt_updated_at is null


