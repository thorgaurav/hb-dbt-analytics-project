with source as (

    select * from {{ source('hubspot_ae', 'CALENDAR') }}

),

deduped as (
    select
        listing_id,
        cast(date as date) as calendar_date,
        case
            when available = 't' then true
            else false end as is_available,
        reservation_id,
        cast(regexp_replace(price, '[^0-9.]', '') as double) as price,
        minimum_nights,
        maximum_nights,
        row_number() over (
            partition by listing_id, date
            order by
                case when reservation_id is not null then 1 else 2 end
        ) as rn
    from source
)

select
    listing_id,
    calendar_date,
    is_available,
    reservation_id,
    price,
    minimum_nights,
    maximum_nights
from deduped
where rn = 1