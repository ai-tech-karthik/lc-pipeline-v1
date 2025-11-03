/*
    Staging model for customer data
    
    Purpose: Clean and normalize customer data from the source layer.
    This is the second layer in the five-layer architecture, applying
    standardization and cleaning transformations.
    
    Layer: Staging (Layer 2)
    Materialization: view (configured in dbt_project.yml)
    Contract: Enforced (defined in _staging.yml)
    
    Transformations:
    - Trim whitespace from all string fields
    - Convert customer_id to integer type
    - Normalize customer_name to lowercase
    - Standardize has_loan_flag to boolean (yes/y/true/1 -> true, no/n/false/0/none -> false)
    - Preserve loaded_at timestamp from source layer
    
    Naming Conventions:
    - customer_id: Primary key (integer)
    - customer_name: Descriptive name field (lowercase string)
    - has_loan_flag: Boolean indicator with _flag suffix
    - loaded_at: Timestamp with _at suffix
*/

with source as (
    select
        customer_id,
        name,
        has_loan,
        loaded_at
    from workspace.default_source.src_customer
),

cleaned as (
    select
        -- Cast customer_id to integer
        cast(customer_id as integer) as customer_id,
        
        -- Trim whitespace and normalize to lowercase
        lower(trim(name)) as customer_name,
        
        -- Standardize boolean: yes/y/true/1 -> true, no/n/false/0/none -> false
        case
            when lower(trim(has_loan)) in ('yes', 'y', 'true', '1') then true
            when lower(trim(has_loan)) in ('no', 'n', 'false', '0', 'none') then false
            else false
        end as has_loan_flag,
        
        -- Preserve loaded_at timestamp
        loaded_at
        
    from source
)

select * from cleaned