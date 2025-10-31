
    
    

select
    account_id as unique_field,
    count(*) as n_records

from lending_club.main_marts.account_summary
where account_id is not null
group by account_id
having count(*) > 1


