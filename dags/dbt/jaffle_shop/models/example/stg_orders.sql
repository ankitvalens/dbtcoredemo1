with
source as (
    select * from {{ source('default','jaffle_shop_orders') }}
),

orders1 as (
    select * from source
)
select * from orders1
