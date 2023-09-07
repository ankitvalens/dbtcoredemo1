

with
source as (
    select * from {{ source('default','jaffle_shop_customers') }}
),

customers as (

    select
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from customers