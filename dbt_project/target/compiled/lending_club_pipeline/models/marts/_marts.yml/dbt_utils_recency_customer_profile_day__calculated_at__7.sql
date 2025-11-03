






with recency as (

    select 

      
      
        max(calculated_at) as most_recent

    from workspace.default_marts.customer_profile

    

)

select

    
    most_recent,
    cast(timestampadd(day, -7, current_timestamp()) as timestamp) as threshold

from recency
where most_recent < cast(timestampadd(day, -7, current_timestamp()) as timestamp)

