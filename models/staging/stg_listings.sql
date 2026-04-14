with source as (

    select * from {{ source('hubspot_ae', 'LISTINGS') }}

)

select
    id as listing_id,
    name as listing_name,
    host_id,
    host_name,
    cast(host_since as date) as host_since,
    neighborhood,
    property_type,
    room_type,
    accommodates,
    bathrooms_text as bathrooms,
    cast(bedrooms as integer) as bedrooms,
    beds,
    cast(amenities as json) as amenities,
    cast(replace(price, '$', '') as double) as price,
    number_of_reviews,
    cast(first_review as date) as first_review,
    cast(last_review as date) as last_review,
    cast(review_scores_rating as double) as review_scores_rating

from source
where id is not null and neighborhood is not null