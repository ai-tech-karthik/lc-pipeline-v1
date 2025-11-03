
    
    



select dbt_updated_at
from workspace.snapshots.snap_account
where dbt_updated_at is null


