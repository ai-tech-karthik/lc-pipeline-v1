
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from workspace.default_marts.customer_profile
where customer_id is not null
group by customer_id
having count(*) > 1


