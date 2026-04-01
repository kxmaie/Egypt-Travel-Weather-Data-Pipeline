{{config (schema='silver_layer')}}


WITH raw_data AS (
select * from raw_data."Bronze_current_weather_data"
),
cleaned_data AS (SELECT ID AS current_weather_id, 
       TRIM(CITY) AS city,
       TO_TIMESTAMP(DATE/1000000000) AS date,
       CAST(temperature AS integer) AS temperature,
       is_day,
       rain,
       snowfall,
       showers,
       CAST(wind_speed AS integer) AS wind_speed,
       CAST(humidity AS integer) AS humidity,
       TO_TIMESTAMP(INGESTED_AT/1000000000) AS ingested_at
       FROM raw_data
       )
select * , avg(temperature) OVER (partition by city) AS avg_temperature,
           avg (wind_speed) OVER (PARTITION BY city) AS avg_wind_speed
FROM CLEANED_DATA
QUALIFY ROW_NUMBER() OVER (PARTITION BY current_weather_id ORDER BY ingested_at DESC) = 1


