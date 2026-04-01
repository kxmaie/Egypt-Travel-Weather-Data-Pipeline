{{ config(schema='silver_layer') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw_data', 'Silver_famous_landmark_data') }}
),

cleaned_data AS (
    SELECT
        CAST(id AS INT) AS landmark_id,
        TRIM(landmark_name) AS landmark_name,
        UPPER(TRIM(city)) AS city,
        category,
        address,
        latitude,
        longitude,
        TO_TIMESTAMP_NTZ(ingested_at / 1000000000) AS ingested_at
    FROM raw_data
),

deduped AS (
    SELECT *
    FROM cleaned_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY city, TRIM(landmark_name)
        ORDER BY ingested_at DESC NULLS LAST
    ) = 1
)

SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY city, COALESCE(category, '')
        ORDER BY landmark_id
    ) AS rank_in_category
FROM deduped
