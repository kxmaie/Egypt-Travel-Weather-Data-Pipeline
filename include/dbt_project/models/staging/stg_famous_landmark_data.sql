{{config (schema='silver_layer')}}

with raw_data as(
    SELECT * FROM {{ source('raw_data', 'Silver_famous_landmark_data')}}
),

cleaned_data as (
        SELECT 
        CAST(id AS INT) as landmark_id,
        TRIM(landmark_name) as landmark_name,
        UPPER(city) as city,
        category,
        address,
        latitude,
        longitude,
        -- التحويل دا عشان كان فى مشكله اما التاريخ انتقل من SQL الى SNOWFLAKE كان بيطلع التاريخ بشكل مش مظبوط
        TO_TIMESTAMP(ingested_at / 1000000000) as ingested_at
    FROM raw_data
)

SELECT * , COUNT(*) OVER (PARTITION BY city, category ORDER BY landmark_id) as rank_in_category
FROM cleaned_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY landmark_id ORDER BY ingested_at DESC) = 1