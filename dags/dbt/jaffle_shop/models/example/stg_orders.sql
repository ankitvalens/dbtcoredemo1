with
orders as (
    select * from {{ source('default','jaffle_shop_orders') }}
),


select * from orders
