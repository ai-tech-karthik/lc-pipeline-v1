-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        workspace.default_intermediate.int_account_with_customer as DBT_INTERNAL_DEST
    using
        int_account_with_customer__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.account_id <=> DBT_INTERNAL_DEST.account_id
          
    when matched
        then update set
            `account_id` = DBT_INTERNAL_SOURCE.`account_id`, `customer_id` = DBT_INTERNAL_SOURCE.`customer_id`, `balance_amount` = DBT_INTERNAL_SOURCE.`balance_amount`, `account_type` = DBT_INTERNAL_SOURCE.`account_type`, `customer_name` = DBT_INTERNAL_SOURCE.`customer_name`, `has_loan_flag` = DBT_INTERNAL_SOURCE.`has_loan_flag`, `valid_from_at` = DBT_INTERNAL_SOURCE.`valid_from_at`, `valid_to_at` = DBT_INTERNAL_SOURCE.`valid_to_at`
    when not matched
        then insert
            (`account_id`, `customer_id`, `balance_amount`, `account_type`, `customer_name`, `has_loan_flag`, `valid_from_at`, `valid_to_at`) VALUES (DBT_INTERNAL_SOURCE.`account_id`, DBT_INTERNAL_SOURCE.`customer_id`, DBT_INTERNAL_SOURCE.`balance_amount`, DBT_INTERNAL_SOURCE.`account_type`, DBT_INTERNAL_SOURCE.`customer_name`, DBT_INTERNAL_SOURCE.`has_loan_flag`, DBT_INTERNAL_SOURCE.`valid_from_at`, DBT_INTERNAL_SOURCE.`valid_to_at`)
