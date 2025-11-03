-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        workspace.default_intermediate.int_savings_account_only as DBT_INTERNAL_DEST
    using
        int_savings_account_only__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.account_id <=> DBT_INTERNAL_DEST.account_id
          
    when matched
        then update set
            `account_id` = DBT_INTERNAL_SOURCE.`account_id`, `customer_id` = DBT_INTERNAL_SOURCE.`customer_id`, `customer_name` = DBT_INTERNAL_SOURCE.`customer_name`, `balance_amount` = DBT_INTERNAL_SOURCE.`balance_amount`, `has_loan_flag` = DBT_INTERNAL_SOURCE.`has_loan_flag`, `valid_from_at` = DBT_INTERNAL_SOURCE.`valid_from_at`, `valid_to_at` = DBT_INTERNAL_SOURCE.`valid_to_at`
    when not matched
        then insert
            (`account_id`, `customer_id`, `customer_name`, `balance_amount`, `has_loan_flag`, `valid_from_at`, `valid_to_at`) VALUES (DBT_INTERNAL_SOURCE.`account_id`, DBT_INTERNAL_SOURCE.`customer_id`, DBT_INTERNAL_SOURCE.`customer_name`, DBT_INTERNAL_SOURCE.`balance_amount`, DBT_INTERNAL_SOURCE.`has_loan_flag`, DBT_INTERNAL_SOURCE.`valid_from_at`, DBT_INTERNAL_SOURCE.`valid_to_at`)
