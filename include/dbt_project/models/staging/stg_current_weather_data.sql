{{ config(schema='silver_layer') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw_data', 'Bronze_current_weather_data') }}
),

cleaned_data AS (
    SELECT
        id AS current_weather_id,
        TRIM(city) AS city,
        {{ normalize_timestamp_ntz('"DATE"') }} AS observed_at,
        temperature::FLOAT AS temperature,
        is_day,
        rain,
        snowfall,
        showers,
        wind_speed::FLOAT AS wind_speed,
        humidity::FLOAT AS humidity,
        {{ normalize_timestamp_ntz('ingested_at') }} AS ingested_at
    FROM raw_data
),

enriched AS (
    SELECT
        *,
        AVG(temperature) OVER (PARTITION BY city) AS avg_temperature,
        AVG(wind_speed) OVER (PARTITION BY city) AS avg_wind_speed
    FROM cleaned_data
)

SELECT *
FROM enriched
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(city)
    ORDER BY ingested_at DESC NULLS LAST
) = 1
