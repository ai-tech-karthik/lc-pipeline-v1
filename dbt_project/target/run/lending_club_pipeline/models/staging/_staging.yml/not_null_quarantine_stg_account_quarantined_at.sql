
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quarantined_at
from workspace.default_staging.quarantine_stg_account
where quarantined_at is null



  
  
      
    ) dbt_internal_test