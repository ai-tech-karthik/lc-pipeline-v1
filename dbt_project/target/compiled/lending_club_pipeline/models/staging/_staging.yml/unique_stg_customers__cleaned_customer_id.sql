
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from lending_club.main_staging.stg_customers__cleaned
where customer_id is not null
group by customer_id
having count(*) > 1


