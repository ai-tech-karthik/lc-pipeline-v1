
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_accounts_count
from workspace.default_marts.customer_profile
where total_accounts_count is null



  
  
      
    ) dbt_internal_test