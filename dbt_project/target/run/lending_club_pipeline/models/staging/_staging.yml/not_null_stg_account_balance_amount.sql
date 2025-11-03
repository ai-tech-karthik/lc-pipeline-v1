
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select balance_amount
from workspace.default_staging.stg_account
where balance_amount is null



  
  
      
    ) dbt_internal_test