-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        workspace.default_marts.account_summary as DBT_INTERNAL_DEST
    using
        account_summary__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.account_id <=> DBT_INTERNAL_DEST.account_id
          
    when matched
        then update set
            `account_id` = DBT_INTERNAL_SOURCE.`account_id`, `customer_id` = DBT_INTERNAL_SOURCE.`customer_id`, `original_balance_amount` = DBT_INTERNAL_SOURCE.`original_balance_amount`, `interest_rate_pct` = DBT_INTERNAL_SOURCE.`interest_rate_pct`, `annual_interest_amount` = DBT_INTERNAL_SOURCE.`annual_interest_amount`, `new_balance_amount` = DBT_INTERNAL_SOURCE.`new_balance_amount`, `calculated_at` = DBT_INTERNAL_SOURCE.`calculated_at`
    when not matched
        then insert
            (`account_id`, `customer_id`, `original_balance_amount`, `interest_rate_pct`, `annual_interest_amount`, `new_balance_amount`, `calculated_at`) VALUES (DBT_INTERNAL_SOURCE.`account_id`, DBT_INTERNAL_SOURCE.`customer_id`, DBT_INTERNAL_SOURCE.`original_balance_amount`, DBT_INTERNAL_SOURCE.`interest_rate_pct`, DBT_INTERNAL_SOURCE.`annual_interest_amount`, DBT_INTERNAL_SOURCE.`new_balance_amount`, DBT_INTERNAL_SOURCE.`calculated_at`)
