with source as (
    select * from {{ ref("stg_orders") }}
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