{{
config(
materialized = 'table'
)
}}
with base as(
    select * from {{ref('fct_listing_day')}}
)
select
    extract(month from calendar_date) as month,
    extract(year from calendar_date) as year,
    SUM(revenue) as total_revenue,
    COUNT(*) as total_listing_days,
    SUM(CASE WHEN is_available then 1 else 0 end) as available_days,
    SUM(CASE WHEN is_booked = 1 then 1 else 0 end) as is_booked
from base
group by
    extract(year from calendar_date),
    extract(month from calendar_date)
order by year, month