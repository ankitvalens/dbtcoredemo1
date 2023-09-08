with source as (
    select * from {{ ref("stg_orders") }}
),

total as (
    SELECT
    order_date,
    COUNT(*) AS total_orders
    FROM
        source
    WHERE order_date = {{ var('business_date') }}
    GROUP BY
        order_date
    ORDER BY
        order_date
)
select * from total