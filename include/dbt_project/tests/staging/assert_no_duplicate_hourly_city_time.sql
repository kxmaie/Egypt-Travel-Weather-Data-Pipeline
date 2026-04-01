-- Fails if more than one row exists for the same city + hourly timestamp.
select
    city,
    date,
    count(*) as row_count
from {{ ref('stg_hourly_weather_data') }}
group by 1, 2
having count(*) > 1
