
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  






with recency as (

    select 

      
      
        max(loaded_at) as most_recent

    from workspace.default_source.src_account

    

)

select

    
    most_recent,
    cast(timestampadd(day, -7, current_timestamp()) as timestamp) as threshold

from recency
where most_recent < cast(timestampadd(day, -7, current_timestamp()) as timestamp)


  
  
      
    ) dbt_internal_test