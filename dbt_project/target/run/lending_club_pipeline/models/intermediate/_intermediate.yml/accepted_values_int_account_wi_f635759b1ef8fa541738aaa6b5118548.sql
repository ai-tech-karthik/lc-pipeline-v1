
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        account_type as value_field,
        count(*) as n_records

    from workspace.default_intermediate.int_account_with_customer
    group by account_type

)

select *
from all_values
where value_field not in (
    'Savings','Checking'
)



  
  
      
    ) dbt_internal_test