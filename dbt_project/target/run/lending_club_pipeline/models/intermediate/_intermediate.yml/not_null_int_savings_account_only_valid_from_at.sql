
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valid_from_at
from workspace.default_intermediate.int_savings_account_only
where valid_from_at is null



  
  
      
    ) dbt_internal_test