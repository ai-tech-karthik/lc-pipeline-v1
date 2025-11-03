
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from workspace.default_source.src_customer

where not(count(*) > 0)


  
  
      
    ) dbt_internal_test