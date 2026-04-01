{{ config(schema='silver_layer') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw_data', 'Bronze_hourly_weather_data') }}
),

cleaned_data AS (
    SELECT 
        id AS hourly_weather_id,
        TRIM(city) AS city,
        
        COALESCE(
            TRY_CAST(CAST("DATE" AS VARCHAR) AS TIMESTAMP_NTZ),
            TO_TIMESTAMP_NTZ(CAST(TRY_TO_DOUBLE(CAST("DATE" AS VARCHAR)) / 1000000000 AS BIGINT))
        ) AS date,

        CAST(temp AS FLOAT) AS temp,
        CAST(humidity AS FLOAT) AS humidity,
        
        rain,
        showers,
        snowfall,

        COALESCE(
            TRY_CAST(CAST(ingested_at AS VARCHAR) AS TIMESTAMP_NTZ),
            TO_TIMESTAMP_NTZ(CAST(TRY_TO_DOUBLE(CAST(ingested_at AS VARCHAR)) / 1000000000 AS BIGINT))
        ) AS ingested_at

    FROM raw_data
)

SELECT * FROM cleaned_data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(city), date
    ORDER BY ingested_at DESC NULLS LAST
) = 1