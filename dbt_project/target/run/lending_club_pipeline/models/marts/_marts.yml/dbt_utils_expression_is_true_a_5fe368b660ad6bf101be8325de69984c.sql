
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from workspace.default_marts.account_summary

where not(original_balance_amount >= 0)


  
  
      
    ) dbt_internal_test