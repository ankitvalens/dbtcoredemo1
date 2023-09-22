create or replace view `default`.`dim_dbt__sources`
  
  
  as
    with base as (

    select *
    from `default`.`stg_dbt__sources`

),

sources as (

    select
        source_execution_id,
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

select * from sources