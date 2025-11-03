
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_annual_interest_amount
from workspace.default_marts.customer_profile
where total_annual_interest_amount is null



  
  
      
    ) dbt_internal_test