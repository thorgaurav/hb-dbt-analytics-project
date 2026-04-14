{% snapshot amenities_snapshot %}

{{
config(
    target_schema = 'main',
    unique_key = 'listing_id',
    strategy = 'timestamp',
    updated_at = 'change_at'
)
}}

select listing_id,
       change_at,
       amenities
from {{source ('hubspot_ae', 'AMENITIES_CHANGELOG')}}

{% endsnapshot %}