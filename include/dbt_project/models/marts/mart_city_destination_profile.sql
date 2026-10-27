{{ config(materialized='table') }}

/*
  Gold layer: one row per city, normalized across Arabic (weather/landmarks)
  and English (restaurants) names via the city_name_mapping seed.
*/

WITH city_universe AS (
    SELECT DISTINCT city_name_normalized AS city
    FROM {{ ref('city_name_mapping') }}
),

latest_daily AS (
    SELECT m.city_name_normalized AS city, d.*
    FROM {{ ref('stg_daily_weather_data') }} d
    JOIN {{ ref('city_name_mapping') }} m ON TRIM(d.city) = TRIM(m.city_name_arabic)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY m.city_name_normalized
        ORDER BY daily_date DESC, ingested_at DESC NULLS LAST
    ) = 1
),

current_wx AS (
    SELECT m.city_name_normalized AS city, c.*
    FROM {{ ref('stg_current_weather_data') }} c
    JOIN {{ ref('city_name_mapping') }} m ON TRIM(c.city) = TRIM(m.city_name_arabic)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY m.city_name_normalized
        ORDER BY ingested_at DESC NULLS LAST
    ) = 1
),

landmark_counts AS (
    SELECT m.city_name_normalized AS city, COUNT(*) AS landmark_count
    FROM {{ ref('stg_famous_landmark_data') }} l
    JOIN {{ ref('city_name_mapping') }} m ON TRIM(l.city) = TRIM(m.city_name_arabic)
    GROUP BY 1
),

restaurant_counts AS (
    SELECT m.city_name_normalized AS city, COUNT(*) AS restaurant_count
    FROM {{ ref('stg_restaurant_data') }} r
    JOIN {{ ref('city_name_mapping') }} m ON TRIM(r.city) = TRIM(m.city_name_english)
    GROUP BY 1
)

SELECT
    u.city,
    d.daily_date,
    d.max_temp AS daily_max_temp_c,
    d.min_temp AS daily_min_temp_c,
    d.max_wind_speed,
    c.temperature AS current_temp_c,
    c.humidity AS current_humidity_pct,
    c.avg_temperature AS city_avg_temp_c,
    COALESCE(l.landmark_count, 0) AS landmark_count,
    COALESCE(r.restaurant_count, 0) AS restaurant_count,
    CURRENT_TIMESTAMP() AS profile_built_at
FROM city_universe u
LEFT JOIN latest_daily d ON u.city = d.city
LEFT JOIN current_wx c ON u.city = c.city
LEFT JOIN landmark_counts l ON u.city = l.city
LEFT JOIN restaurant_counts r ON u.city = r.city
WHERE u.city IS NOT NULL AND TRIM(u.city) != ''
