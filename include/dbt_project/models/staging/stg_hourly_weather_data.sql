{{ config(schema='silver_layer') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw_data', 'Bronze_hourly_weather_data') }}
),

cleaned_data AS (
    SELECT 
        id AS hourly_weather_id,
        TRIM(city) AS city,
        
        {{ normalize_timestamp_ntz('"DATE"') }} AS date,

        CAST(temp AS FLOAT) AS temp,
        CAST(humidity AS FLOAT) AS humidity,
        
        rain,
        showers,
        snowfall,

        {{ normalize_timestamp_ntz('ingested_at') }} AS ingested_at

    FROM raw_data
)

SELECT * FROM cleaned_data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(city), date
    ORDER BY ingested_at DESC NULLS LAST
) = 1

