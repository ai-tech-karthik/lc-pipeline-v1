
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

-- Test that SCD2 records do not have overlapping validity periods
-- For each unique_key, verify that no two versions have overlapping
-- dbt_valid_from and dbt_valid_to timestamps

with snapshot_records as (
    select
        customer_id,
        dbt_valid_from,
        dbt_valid_to,
        dbt_scd_id
    from workspace.snapshots.snap_customer
),

overlaps as (
    select
        a.customer_id,
        a.dbt_scd_id as record_a_id,
        a.dbt_valid_from as a_valid_from,
        a.dbt_valid_to as a_valid_to,
        b.dbt_scd_id as record_b_id,
        b.dbt_valid_from as b_valid_from,
        b.dbt_valid_to as b_valid_to
    from snapshot_records a
    inner join snapshot_records b
        on a.customer_id = b.customer_id
        and a.dbt_scd_id != b.dbt_scd_id
    where
        -- Check for overlapping periods
        -- Two periods overlap if: a_start < b_end AND b_start < a_end
        a.dbt_valid_from < coalesce(b.dbt_valid_to, cast('2099-12-31' as timestamp))
        and b.dbt_valid_from < coalesce(a.dbt_valid_to, cast('2099-12-31' as timestamp))
)

select * from overlaps


  
  
      
    ) dbt_internal_test