with
source as (
    select * from {{ source('dbt_airlinedemodb','raw_flights') }}
),

nv_sri_flights as (
    select (Distance * 1.6) as distance_in_km, * from source where ArrTime < 2400 and ArrTime is not null
)

select * from nv_sri_flights
