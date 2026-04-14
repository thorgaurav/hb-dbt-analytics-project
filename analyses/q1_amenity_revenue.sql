-- Business Problem 1: Amenity Revenue
-- Total Revenue and % of revenue by month segemented by air conditioning presence.

with monthly_revenue as (
    select
        date_trunc('month', calendar_date) as month_start,
        has_ac,
        sum(revenue) as segement_revenue
    from {{ref('fct_listing_day')}}
    group by 1, 2
),
monthly_totals as(
    select
        month_start,
        sum(segment_revenue) as total_monthly_revenue
    from monthly_revenue
    group by 1
)
select
    mr.month_start,
    mr.has_ac,
    mr.segment_revenue,
    mr.total_month_revenue,
    round(100.0 * mr.segment_revenue / nullif(mt.total_monthly_revenue, 0),1) as pct_of_monthly_revenue
from monthly_revenue mr
inner join monthly_totals mt using (month_start)
order by mr.month_start, mr.has_ac desc