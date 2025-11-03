






with recency as (

    select 

      
      
        max(dbt_valid_from) as most_recent

    from workspace.snapshots.snap_customer

    

)

select

    
    most_recent,
    cast(timestampadd(day, -7, current_timestamp()) as timestamp) as threshold

from recency
where most_recent < cast(timestampadd(day, -7, current_timestamp()) as timestamp)

