{{ config(tags=["customer"]) }}

with source as (
    select * from {{ source('default','jaffle_shop_customers') }}
),
customers1 as (
    select * from source
)

select * from customers1