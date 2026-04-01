-- Fails if more than one row exists for the same city + calendar day.
select
    city,
    daily_date,
    count(*) as row_count
from {{ ref('stg_daily_weather_data') }}
group by 1, 2
having count(*) > 1
