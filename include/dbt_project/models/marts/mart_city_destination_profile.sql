{{ config(materialized='table') }}

/*
  Gold layer: one row per city (Arabic names from weather/landmarks; English
  city names from restaurants only match when identical — consider a mapping
  seed for production).
*/

WITH city_universe AS (
    SELECT DISTINCT TRIM(city) AS city FROM {{ ref('stg_daily_weather_data') }}
    UNION
    SELECT DISTINCT TRIM(city) AS city FROM {{ ref('stg_famous_landmark_data') }}
    UNION
    SELECT DISTINCT TRIM(city) AS city FROM {{ ref('stg_restaurant_data') }}
),

latest_daily AS (
    SELECT *
    FROM {{ ref('stg_daily_weather_data') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRIM(city)
        ORDER BY daily_date DESC, ingested_at DESC NULLS LAST
    ) = 1
),

current_wx AS (
    SELECT *
    FROM {{ ref('stg_current_weather_data') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRIM(city)
        ORDER BY ingested_at DESC NULLS LAST
    ) = 1
),

landmark_counts AS (
    SELECT TRIM(city) AS city, COUNT(*) AS landmark_count
    FROM {{ ref('stg_famous_landmark_data') }}
    GROUP BY 1
),

restaurant_counts AS (
    SELECT TRIM(city) AS city, COUNT(*) AS restaurant_count
    FROM {{ ref('stg_restaurant_data') }}
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
    COALESCE(l.landmark_count, 0) AS landmark_count,
    COALESCE(r.restaurant_count, 0) AS restaurant_count,
    CURRENT_TIMESTAMP() AS profile_built_at
FROM city_universe u
LEFT JOIN latest_daily d ON TRIM(u.city) = TRIM(d.city)
LEFT JOIN current_wx c ON TRIM(u.city) = TRIM(c.city)
LEFT JOIN landmark_counts l ON TRIM(u.city) = TRIM(l.city)
LEFT JOIN restaurant_counts r ON TRIM(u.city) = TRIM(r.city)
WHERE u.city IS NOT NULL AND TRIM(u.city) != ''
