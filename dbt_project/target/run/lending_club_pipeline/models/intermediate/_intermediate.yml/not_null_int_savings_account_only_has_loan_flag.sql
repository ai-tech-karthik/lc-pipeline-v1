
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_loan_flag
from workspace.default_intermediate.int_savings_account_only
where has_loan_flag is null



  
  
      
    ) dbt_internal_test