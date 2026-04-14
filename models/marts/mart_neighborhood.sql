{{config(materialized='table')}}
with base as
(
select * from {{ref('fct_listing_day')}}
)

select
    neighborhood,
    sum(revenue) as total_revenue,
    avg(price) as avg_price,
    count(*) as total_days
from base
group by neighborhood