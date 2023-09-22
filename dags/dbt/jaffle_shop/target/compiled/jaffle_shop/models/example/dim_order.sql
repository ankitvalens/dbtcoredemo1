with source as (
    select * from `default`.`stg_orders`
),

total as (
    SELECT
    order_date,
    COUNT(*) AS total_orders
    FROM
        source
    -- WHERE order_date = 2023-09-06
    GROUP BY
        order_date
    ORDER BY
        order_date
)
select * from total