
    
    

with all_values as (

    select
        account_type as value_field,
        count(*) as n_records

    from lending_club.main_staging.stg_accounts__cleaned
    group by account_type

)

select *
from all_values
where value_field not in (
    'Savings','Checking'
)


