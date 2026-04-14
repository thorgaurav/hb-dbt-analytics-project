{{config(materialized = 'table')}}
with base as(
    select * from {{ref('fct_listing_day')}}
)

select has_ac,
       count(*) as total_days,
       sum(revenue) as total_revenue,
       avg(price) as avg_price
from base
group by has_ac