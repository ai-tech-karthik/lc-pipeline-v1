
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select interest_rate_pct
from workspace.default_marts.account_summary
where interest_rate_pct is null



  
  
      
    ) dbt_internal_test