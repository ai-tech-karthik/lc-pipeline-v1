
    
    

select
    dbt_scd_id as unique_field,
    count(*) as n_records

from workspace.snapshots.snap_account
where dbt_scd_id is not null
group by dbt_scd_id
having count(*) > 1


