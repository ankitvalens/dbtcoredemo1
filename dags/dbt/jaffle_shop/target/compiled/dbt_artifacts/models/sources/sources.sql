/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (
    select 1 as foo
)

select
    cast(null as string) as command_invocation_id,
    cast(null as string) as node_id,
    cast(null as timestamp) as run_started_at,
    cast(null as string) as database,
    cast(null as string) as schema,
    cast(null as string) as source_name,
    cast(null as string) as loader,
    cast(null as string) as name,
    cast(null as string) as identifier,
    cast(null as string) as loaded_at_field,
    
        cast(null as string) as freshness,
    
    cast(null as string) as all_results
from dummy_cte
where 1 = 0