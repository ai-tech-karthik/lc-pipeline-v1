
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select original_balance_amount
from workspace.default_marts.account_summary
where original_balance_amount is null



  
  
      
    ) dbt_internal_test