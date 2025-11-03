






with recency as (

    select 

      
      
        max(valid_from_at) as most_recent

    from workspace.default_intermediate.int_savings_account_only

    

)

select

    
    most_recent,
    cast(timestampadd(day, -7, current_timestamp()) as timestamp) as threshold

from recency
where most_recent < cast(timestampadd(day, -7, current_timestamp()) as timestamp)

