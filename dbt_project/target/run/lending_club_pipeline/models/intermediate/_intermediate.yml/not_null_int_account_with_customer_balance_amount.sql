
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select balance_amount
from workspace.default_intermediate.int_account_with_customer
where balance_amount is null



  
  
      
    ) dbt_internal_test