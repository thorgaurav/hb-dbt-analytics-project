with calendar as (
    select * from {{ ref('stg_calendar') }}
)

select
    listing_id,
    calendar_date,
    price,
    is_available,
    reservation_id,
    case
        when reservation_id is not null then 1
        else 0
    end as is_booked,
    case
        when reservation_id is not null then price
        else 0
    end as revenue,
    minimum_nights,
    maximum_nights
from calendar