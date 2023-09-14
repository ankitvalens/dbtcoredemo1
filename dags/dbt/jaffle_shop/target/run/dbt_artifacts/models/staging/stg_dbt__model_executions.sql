create or replace view `default`.`stg_dbt__model_executions`
  
  
  as
    with base as (

    select *
    from `default`.`model_executions`

),

enhanced as (

    select
        
md5(cast(concat(coalesce(cast(command_invocation_id as string), ''), '-', coalesce(cast(node_id as string), '')) as string)) as model_execution_id,
        command_invocation_id,
        node_id,
        run_started_at,
        was_full_refresh,
        

        split(
            thread_id,
            

        -- escape if starts with a special character
        case when regexp_extract('-', '([^A-Za-z0-9])(.*)', 1) != '_'
            then concat('\\', '-')
            else '-' end

    
            )[(1)]

         as thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected,
        
        materialization,
        schema, -- noqa
        name,
        alias,
        message,
        adapter_response
    from base

)

select * from enhanced
