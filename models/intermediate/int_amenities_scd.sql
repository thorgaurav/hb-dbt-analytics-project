with base as (
    select
        listing_id,
        cast(change_at as date) as valid_from,
        amenities,
        lead(cast(change_at as date)) over (
            partition by listing_id
            order by change_at
        ) as next_change
    from {{ ref('stg_amenities_changelog') }}
)
select
    listing_id,
    valid_from,
    coalesce((next_change - interval '1 day')::DATE, '9999-12-31'::DATE) as valid_to,
    amenities
from base