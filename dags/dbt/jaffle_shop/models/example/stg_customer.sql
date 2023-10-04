{{ config(tags=["customer"]) }}

with source1 as (
    select * from {{ source('default','jaffle_shop_customers') }}
),
customers1 as (
    select * from sourece1
)

select * from customers1