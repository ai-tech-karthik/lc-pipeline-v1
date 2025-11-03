
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select account_type
from workspace.snapshots.snap_account
where account_type is null



  
  
      
    ) dbt_internal_test