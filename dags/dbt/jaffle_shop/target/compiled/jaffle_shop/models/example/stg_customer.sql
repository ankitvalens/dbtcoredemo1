

with
source as (
    select * from `hive_metastore`.`default`.`jaffle_shop_customers`
)

select * from customer