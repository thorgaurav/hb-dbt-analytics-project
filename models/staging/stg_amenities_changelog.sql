with source as (

    select * from {{ source('hubspot_ae', 'AMENITIES_CHANGELOG') }}

)
select
    listing_id,
    cast(change_at as timestamp) as change_at,
    cast(amenities as json) as amenities
from source