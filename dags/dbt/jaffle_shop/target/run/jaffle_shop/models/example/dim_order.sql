create or replace view `default`.`dim_order`
  
  
  as
    with source as (
    select * from `default`.`stg_orders`
),

total as (
    SELECT
    order_date,
    COUNT(*) AS total_orders
    FROM
        source
    GROUP BY
        order_date
    ORDER BY
        order_date
)
select * from total
