
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select balance_amount
from workspace.default_intermediate.int_savings_account_only
where balance_amount is null



  
  
      
    ) dbt_internal_test