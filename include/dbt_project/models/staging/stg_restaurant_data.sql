{{ config(schema='silver_layer') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw_data', 'Bronze_restaurant_data') }}
),

cleaned_data AS (
    SELECT
        id AS restaurant_row_id,
        resturantid AS restaurant_source_id,
        TRIM(restaurant_name) AS restaurant_name,
        TRIM(category) AS category,
        TRIM(city) AS city,
        TRIM(area) AS area,
        longitude,
        latitude,
        TO_TIMESTAMP_NTZ(ingested_at / 1000000000) AS ingested_at
    FROM raw_data
)

SELECT *
FROM cleaned_data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY restaurant_source_id, TRIM(city)
    ORDER BY ingested_at DESC NULLS LAST
) = 1
