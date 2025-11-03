
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select account_type
from workspace.default_intermediate.int_account_with_customer
where account_type is null



  
  
      
    ) dbt_internal_test