{{ config(schema='silver_layer') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw_data', 'Bronze_hotel_data') }}
),

cleaned_data AS (
    SELECT
        CAST(id AS INTEGER) AS hotel_id,
        TRIM(type) AS type,
        TRIM(name) AS hotel_name,
        COALESCE(link, 'unknown') AS link_for_the_hotel,
        latitude,
        longitude,
        COALESCE(check_in_time, 'unknown') AS check_in_time,
        COALESCE(check_out_time, 'unknown') AS check_out_time,
        COALESCE(NULLIF(reviews, 0) * NULLIF(overall_rating, 0), 0) AS weighted_score,
        COALESCE(TRIM(REPLACE(amenities, ',', '-')), 'unknown') AS amenities,
        COALESCE(TRIM(REPLACE(excluded_amenities, ',', '-')), 'unknown') AS excluded_amenities,
        TRIM(SPLIT_PART(nearby_places, ':', 1)) AS nearby_places,
        COALESCE(
            CAST(REGEXP_REPLACE(rate_per_night_lowest, '[^0-9.]', '') AS FLOAT),
            0
        ) AS final_price,
        COALESCE(
            CAST(REGEXP_REPLACE(rate_per_night_before_taxes_and_fees, '[^0-9.]', '') AS FLOAT),
            0
        ) AS base_price,
        TO_TIMESTAMP_NTZ(ingested_at / 1000000000) AS ingested_at
    FROM raw_data
),

enriched AS (
    SELECT
        *,
        COALESCE(final_price - base_price, 0) AS total_taxes,
        ROW_NUMBER() OVER (ORDER BY weighted_score DESC, hotel_id ASC) AS recommended_rank
    FROM cleaned_data
)

SELECT *
FROM enriched
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(LOWER(hotel_name)), latitude, longitude
    ORDER BY ingested_at DESC NULLS LAST
) = 1
