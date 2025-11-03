
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select failure_reason
from workspace.default_staging.quarantine_stg_customer
where failure_reason is null



  
  
      
    ) dbt_internal_test