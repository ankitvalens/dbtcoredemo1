with stg_nv_sri_flights_count as (
    select
        count(1) as source,
        'count check' as recon_metric,
        origin, dest
    from
        {{ ref('brz_flights') }} 
    group by
        origin, dest
),
f_flights__count as (
    select
        count(1) as target,
        'count check' as recon_metric,
        origin, dest
    from
        {{ ref('fnd_flights') }} 
    group by
        origin, dest
),
stg_nv_sri_flights_distance as (
    select
        sum(distance) as source,
        'sum of distance' as recon_metric,
        origin, dest
    from
        {{ ref('brz_flights') }} 
    group by
        origin, dest
),
f_flights__distance as (
    select
        sum(distance) as target,
        'sum of distance' as recon_metric,
        origin, dest
    from
        {{ ref('fnd_flights') }} 
    group by
        origin, dest
),
final_result as (
    select * from stg_nv_sri_flights_count
    union all
    select * from f_flights__count
    union all
    select * from stg_nv_sri_flights_distance
    union all
    select * from f_flights__distance
)
select *,CURRENT_TIMESTAMP as created_at from final_result