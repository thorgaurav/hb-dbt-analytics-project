-- Business Problem #2: Neighborhood Pricing
-- ────────────────────────────────────────────
-- Average price increase per neighborhood from 2021-07-12 to 2022-07-11.
--

with start_prices as (

    select
        listing_id,
        neighborhood,
        price                                       as price_start
    from {{ ref('fct_listing_day') }}
    where calendar_date = '2021-07-12'
      and neighborhood is not null
      and trim(neighborhood) != ''

),

end_prices as (

    select
        listing_id,
        neighborhood,
        price                                       as price_end
    from {{ ref('fct_listing_day') }}
    where calendar_date = '2022-07-11'
      and neighborhood is not null
      and trim(neighborhood) != ''

),

per_listing_delta as (

    select
        s.listing_id,
        s.neighborhood,
        s.price_start,
        e.price_end,
        e.price_end - s.price_start                as price_delta
    from start_prices s
    inner join end_prices e on s.listing_id = e.listing_id

)

select
    neighborhood,
    round(avg(price_delta), 2)                      as avg_price_increase,
    count(*)                                        as listing_count
from per_listing_delta
where neighborhood is not null
group by neighborhood
order by avg_price_increase desc