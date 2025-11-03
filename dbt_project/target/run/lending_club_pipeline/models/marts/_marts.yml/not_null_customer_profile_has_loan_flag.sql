
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_loan_flag
from workspace.default_marts.customer_profile
where has_loan_flag is null



  
  
      
    ) dbt_internal_test