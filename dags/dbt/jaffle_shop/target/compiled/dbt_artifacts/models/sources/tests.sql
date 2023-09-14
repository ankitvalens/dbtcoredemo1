/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (
    select 1 as foo
)

select
    cast(null as string) as command_invocation_id,
    cast(null as string) as node_id,
    cast(null as timestamp) as run_started_at,
    cast(null as string) as name,
    cast(null as string) as depends_on_nodes,
    cast(null as string) as package_name,
    cast(null as string) as test_path,
    cast(null as string) as tags,
    cast(null as string) as all_results
from dummy_cte
where 1 = 0