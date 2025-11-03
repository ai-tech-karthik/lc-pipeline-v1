
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dbt_updated_at
from workspace.snapshots.snap_customer
where dbt_updated_at is null



  
  
      
    ) dbt_internal_test