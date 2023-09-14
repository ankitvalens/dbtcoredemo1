-- back compat for old kwarg name
  
  
  
      
  

  

  merge into `default`.`snapshot_executions` as DBT_INTERNAL_DEST
      using `snapshot_executions__dbt_tmp` as DBT_INTERNAL_SOURCE
      on FALSE

      when matched then update set
         * 

      when not matched then insert *
