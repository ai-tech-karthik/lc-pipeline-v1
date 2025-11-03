-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        workspace.default_marts.customer_profile as DBT_INTERNAL_DEST
    using
        customer_profile__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.customer_id <=> DBT_INTERNAL_DEST.customer_id
          
    when matched
        then update set
            `customer_id` = DBT_INTERNAL_SOURCE.`customer_id`, `customer_name` = DBT_INTERNAL_SOURCE.`customer_name`, `has_loan_flag` = DBT_INTERNAL_SOURCE.`has_loan_flag`, `total_accounts_count` = DBT_INTERNAL_SOURCE.`total_accounts_count`, `total_balance_amount` = DBT_INTERNAL_SOURCE.`total_balance_amount`, `total_annual_interest_amount` = DBT_INTERNAL_SOURCE.`total_annual_interest_amount`, `calculated_at` = DBT_INTERNAL_SOURCE.`calculated_at`
    when not matched
        then insert
            (`customer_id`, `customer_name`, `has_loan_flag`, `total_accounts_count`, `total_balance_amount`, `total_annual_interest_amount`, `calculated_at`) VALUES (DBT_INTERNAL_SOURCE.`customer_id`, DBT_INTERNAL_SOURCE.`customer_name`, DBT_INTERNAL_SOURCE.`has_loan_flag`, DBT_INTERNAL_SOURCE.`total_accounts_count`, DBT_INTERNAL_SOURCE.`total_balance_amount`, DBT_INTERNAL_SOURCE.`total_annual_interest_amount`, DBT_INTERNAL_SOURCE.`calculated_at`)
