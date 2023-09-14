-- back compat for old kwarg name
  
  
  
      
  

  

  merge into `default`.`seed_executions` as DBT_INTERNAL_DEST
      using `seed_executions__dbt_tmp` as DBT_INTERNAL_SOURCE
      on FALSE

      when matched then update set
         * 

      when not matched then insert *
