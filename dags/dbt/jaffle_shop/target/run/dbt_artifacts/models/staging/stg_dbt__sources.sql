create or replace view `default`.`stg_dbt__sources`
  
  
  as
    with base as (

    select *
    from `default`.`sources`

),

enhanced as (

    select
        
md5(cast(concat(coalesce(cast(command_invocation_id as string), ''), '-', coalesce(cast(node_id as string), '')) as string)) as source_execution_id,
        command_invocation_id,
        node_id,
        run_started_at,
        database,
        schema,
        source_name,
        loader,
        name,
        identifier,
        loaded_at_field,
        freshness
    from base

)

select * from enhanced
