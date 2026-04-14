{{
config(
materialized = 'incremental',
unique_key = dbt_utils.generate_surrogate_key(['listing_id', 'calendar_date'])
)
}}


with calendar as (
    select * from {{ ref('int_calendar_enriched') }}
),

listings as (
    select * from {{ ref('stg_listings') }}
),

amenities as (
    select * from {{ ref('int_amenities_scd') }}
)

select
    {{dbt_utils.generate_surrogate_key(['c.listing_id', 'c.calendar_date'])}} as listing_day_sk,
    c.listing_id,
    c.calendar_date,
    l.neighborhood,
    l.property_type,
    l.room_type,
    l.accommodates,
    c.price,
    c.is_available,
    c.is_booked,
    c.revenue,
    c.minimum_nights,
    c.maximum_nights,
    a.amenities,
    coalesce(list_contains(cast(json_extract(a.amenities, '$') as varchar[]),'Air conditioning'),false) as has_ac,
    coalesce(list_contains(cast(json_extract(a.amenities, '$') as varchar[]),'Wifi'),false) as has_wifi,
    coalesce(list_contains(cast(json_extract(a.amenities, '$') as varchar[]),'Lockbox'),false) as has_lockbox,
    coalesce(list_contains(cast(json_extract(a.amenities, '$') as varchar[]),'First aid kit'),false) as has_first_aid

from calendar c

inner join listings l
    on c.listing_id = l.listing_id

inner join amenities a
    on c.listing_id = a.listing_id
   and c.calendar_date >= a.valid_from
   and c.calendar_date <= a.valid_to
where 1=1

{% if is_incremental() %}
and c.calendar_date > (
select max(calendar_date) from {{this}}
)
{% endif %}