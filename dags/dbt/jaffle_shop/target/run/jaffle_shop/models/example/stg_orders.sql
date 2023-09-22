create or replace view `default`.`stg_orders`
  
  
  as
    with
orders as (
    select * from `hive_metastore`.`default`.`jaffle_shop_orders`
),


select * from orders
