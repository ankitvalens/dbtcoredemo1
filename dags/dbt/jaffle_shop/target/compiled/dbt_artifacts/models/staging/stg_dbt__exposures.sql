with base as (

    select *
    from `default`.`exposures`

),

enhanced as (

    select
        
md5(cast(concat(coalesce(cast(command_invocation_id as string), ''), '-', coalesce(cast(node_id as string), '')) as string)) as exposure_execution_id,
        command_invocation_id,
        node_id,
        run_started_at,
        name,
        type,
        owner,
        maturity,
        path,
        description,
        url,
        package_name,
        depends_on_nodes,
        tags
    from base

)

select * from enhanced