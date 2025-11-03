
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select account_id
from workspace.default_intermediate.int_account_with_customer
where account_id is null



  
  
      
    ) dbt_internal_test