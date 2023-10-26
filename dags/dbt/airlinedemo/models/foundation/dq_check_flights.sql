
with issue_date as (
    select
        count(*) as count,manufacturer,year
    from
        {{ ref('fnd_planes') }} 
    where
        TO_DATE(CAST(UNIX_TIMESTAMP(issue_date, 'MM/dd/yyyy') AS timestamp)) >= cast('1995-01-01' as date)
    group by 
        manufacturer, year
),
manufacturer_null as (
    select
        count(*) as count,manufacturer,year
    from
        {{ ref('fnd_planes') }} 
    where
        manufacturer is not null
    group by 
        manufacturer, year
),
all_Data as (
select
    'Issue date from year 1995' as dq_col,
    count as count,
    manufacturer,year
from
    issue_date
union all
select
    'Manufacturer is non blank' as dq_col,
    count as count,manufacturer,year
from
    manufacturer_null
)

select *,CURRENT_TIMESTAMP as created_at from all_Data
