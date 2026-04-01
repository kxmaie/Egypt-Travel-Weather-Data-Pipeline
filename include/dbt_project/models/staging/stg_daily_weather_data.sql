{{ config(schema='silver_layer') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw_data', 'Bronze_daily_weather_data') }}
),

cleaned_data AS (
    SELECT
        id AS daily_id,
        TRIM(city) AS city,
        CAST("DATE" AS DATE) AS daily_date,
        temp_max AS max_temp,
        temp_min AS min_temp,
        wind_speed_max AS max_wind_speed,
        TO_TIMESTAMP_NTZ(ingested_at / 1000000000) AS ingested_at
    FROM raw_data
)

SELECT *
FROM cleaned_data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(city), daily_date
    ORDER BY ingested_at DESC NULLS LAST
) = 1
