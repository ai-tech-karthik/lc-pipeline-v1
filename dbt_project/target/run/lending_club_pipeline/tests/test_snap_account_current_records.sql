
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test that current account records have null dbt_valid_to
-- Current records should always have dbt_valid_to IS NULL

select
    account_id,
    dbt_scd_id,
    dbt_valid_from,
    dbt_valid_to
from workspace.snapshots.snap_account
where dbt_valid_to is null
  and dbt_valid_from > current_timestamp
  
  
      
    ) dbt_internal_test