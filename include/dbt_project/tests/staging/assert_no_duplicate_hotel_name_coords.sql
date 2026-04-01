-- Fails if more than one row exists for the same name + coordinates (dedupe grain).
select
    trim(lower(hotel_name)) as hotel_key,
    latitude,
    longitude,
    count(*) as row_count
from {{ ref('stg_hotel_data') }}
group by 1, 2, 3
having count(*) > 1
