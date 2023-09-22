{{ config(tags=["customer"]) }}

with
source as (
    select * from {{ source('default','jaffle_shop_customers') }}
)

select * from customer