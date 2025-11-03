
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_name
from workspace.default_marts.customer_profile
where customer_name is null



  
  
      
    ) dbt_internal_test