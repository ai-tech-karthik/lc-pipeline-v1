
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from workspace.default_marts.customer_profile

where not(total_accounts_count > 0)


  
  
      
    ) dbt_internal_test