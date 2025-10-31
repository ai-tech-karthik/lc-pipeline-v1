
    
    

with all_values as (

    select
        has_loan as value_field,
        count(*) as n_records

    from lending_club.main_staging.stg_customers__cleaned
    group by has_loan

)

select *
from all_values
where value_field not in (
    'Yes','No'
)


