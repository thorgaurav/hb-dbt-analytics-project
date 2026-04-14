-- Business Problem #3: Long Stay / Picky Renter
-- ───────────────────────────────────────────────
-- Longest possible consecutive stay for listings that have BOTH a lockbox
-- AND a first aid kit, respecting availability windows and maximum_nights limits.

with qualified_dates as (

    select
        listing_id,
        calendar_date,
        maximum_nights
    from {{ ref('fct_listing_day') }}
    where
        is_available  = true
        and has_lockbox     = true
        and has_first_aid   = true

),

island_detection as (

    select
        listing_id,
        calendar_date,
        maximum_nights,
        calendar_date - cast(
            row_number() over (
                partition by listing_id
                order by calendar_date
            ) as integer
        ) * interval '1 day'                        as island_key
    from qualified_dates

),

islands as (

    select
        listing_id,
        island_key,
        min(calendar_date)                          as window_start,
        max(calendar_date)                          as window_end,
        count(*)                                    as consecutive_days,
        min(maximum_nights)                         as max_nights_cap
    from island_detection
    group by listing_id, island_key

),

capped_stays as (

    select
        listing_id,
        window_start,
        window_end,
        consecutive_days,
        max_nights_cap,
        least(consecutive_days, max_nights_cap)     as longest_allowed_stay
    from islands

)

select
    listing_id,
    max(longest_allowed_stay)                       as max_possible_stay_days,
    arg_max(window_start, longest_allowed_stay)     as best_window_start,
    arg_max(window_end,   longest_allowed_stay)     as best_window_end
from capped_stays
group by listing_id
order by max_possible_stay_days desc