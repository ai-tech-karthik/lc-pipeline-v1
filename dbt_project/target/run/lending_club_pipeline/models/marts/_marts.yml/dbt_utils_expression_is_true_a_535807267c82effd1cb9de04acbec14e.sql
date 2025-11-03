
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from workspace.default_marts.account_summary

where not(interest_rate_pct >= 0.01 and <= 0.025)


  
  
      
    ) dbt_internal_test