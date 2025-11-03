
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dbt_valid_from
from workspace.snapshots.snap_customer
where dbt_valid_from is null



  
  
      
    ) dbt_internal_test