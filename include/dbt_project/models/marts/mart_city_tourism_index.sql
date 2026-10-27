{{ config(materialized='table') }}

/*
  Gold layer: City Tourism Index — a composite score per city combining
  weather comfort, landmark density, restaurant variety, and hotel availability.

  Score components (each 0–100):
    - Weather comfort    : based on current temperature proximity to 22°C and low wind
    - Landmark density   : normalized count of landmarks
    - Restaurant variety  : normalized count of restaurants
    - Hotel value index   : average value_score across hotels near the city

  Final tourism_index = weighted average of the four components.
*/

WITH base_profile AS (
    SELECT * FROM {{ ref('mart_city_destination_profile') }}
),

hotel_agg AS (
    SELECT
        nearest_city,
        COUNT(*) AS hotel_count,
        AVG(final_price) AS avg_price_per_night,
        AVG(weighted_score) AS avg_hotel_score,
        AVG(value_score) AS avg_value_score
    FROM {{ ref('mart_hotel_analysis') }}
    GROUP BY 1
),

weather_score AS (
    SELECT
        city,
        -- Comfort score: ideal temp 18–26°C, penalise extreme wind
        CASE
            WHEN current_temp_c IS NULL THEN 50
            WHEN current_temp_c BETWEEN 18 AND 26 THEN 100
            WHEN current_temp_c BETWEEN 12 AND 18 OR current_temp_c BETWEEN 26 AND 32 THEN 75
            WHEN current_temp_c BETWEEN 5 AND 12 OR current_temp_c BETWEEN 32 AND 38 THEN 50
            ELSE 25
        END AS weather_comfort_score,
        CASE
            WHEN max_wind_speed IS NULL THEN 50
            WHEN max_wind_speed < 20 THEN 100
            WHEN max_wind_speed < 40 THEN 75
            WHEN max_wind_speed < 60 THEN 50
            ELSE 25
        END AS wind_score
    FROM base_profile
),

max_landmarks AS (
    SELECT MAX(landmark_count) AS max_l FROM base_profile
),
max_restaurants AS (
    SELECT MAX(restaurant_count) AS max_r FROM base_profile
),
max_hotel_value AS (
    SELECT MAX(avg_value_score) AS max_v FROM hotel_agg WHERE avg_value_score IS NOT NULL
)

SELECT
    b.city,
    b.daily_max_temp_c,
    b.daily_min_temp_c,
    b.current_temp_c,
    b.current_humidity_pct,
    b.landmark_count,
    b.restaurant_count,
    COALESCE(h.hotel_count, 0) AS hotel_count,
    ROUND(COALESCE(h.avg_price_per_night, 0), 2) AS avg_hotel_price_per_night,
    ROUND(COALESCE(h.avg_hotel_score, 0), 2) AS avg_hotel_score,
    -- Component scores (0–100)
    ROUND(ws.weather_comfort_score, 1) AS weather_comfort_score,
    ROUND(ws.wind_score, 1) AS wind_score,
    ROUND(
        CASE
            WHEN ml.max_l > 0 THEN (b.landmark_count * 100.0 / ml.max_l)
            ELSE 0
        END, 1
    ) AS landmark_score,
    ROUND(
        CASE
            WHEN mr.max_r > 0 THEN (b.restaurant_count * 100.0 / mr.max_r)
            ELSE 0
        END, 1
    ) AS restaurant_score,
    ROUND(
        CASE
            WHEN mv.max_v > 0 AND h.avg_value_score IS NOT NULL
                THEN (h.avg_value_score * 100.0 / mv.max_v)
            ELSE 0
        END, 1
    ) AS hotel_value_score,
    -- Composite tourism index (weighted average)
    ROUND(
          0.35 * ws.weather_comfort_score
        + 0.15 * ws.wind_score
        + 0.25 * CASE WHEN ml.max_l > 0 THEN (b.landmark_count * 100.0 / ml.max_l) ELSE 0 END
        + 0.15 * CASE WHEN mr.max_r > 0 THEN (b.restaurant_count * 100.0 / mr.max_r) ELSE 0 END
        + 0.10 * CASE WHEN mv.max_v > 0 AND h.avg_value_score IS NOT NULL THEN (h.avg_value_score * 100.0 / mv.max_v) ELSE 0 END
    , 1) AS tourism_index,
    -- Tier
    CASE
        WHEN 0.35 * ws.weather_comfort_score
           + 0.15 * ws.wind_score
           + 0.25 * CASE WHEN ml.max_l > 0 THEN (b.landmark_count * 100.0 / ml.max_l) ELSE 0 END
           + 0.15 * CASE WHEN mr.max_r > 0 THEN (b.restaurant_count * 100.0 / mr.max_r) ELSE 0 END
           + 0.10 * CASE WHEN mv.max_v > 0 AND h.avg_value_score IS NOT NULL THEN (h.avg_value_score * 100.0 / mv.max_v) ELSE 0 END
        >= 75 THEN 'Top Destination'
        WHEN 0.35 * ws.weather_comfort_score
           + 0.15 * ws.wind_score
           + 0.25 * CASE WHEN ml.max_l > 0 THEN (b.landmark_count * 100.0 / ml.max_l) ELSE 0 END
           + 0.15 * CASE WHEN mr.max_r > 0 THEN (b.restaurant_count * 100.0 / mr.max_r) ELSE 0 END
           + 0.10 * CASE WHEN mv.max_v > 0 AND h.avg_value_score IS NOT NULL THEN (h.avg_value_score * 100.0 / mv.max_v) ELSE 0 END
        >= 50 THEN 'Good Destination'
        ELSE 'Emerging Destination'
    END AS destination_tier,
    CURRENT_TIMESTAMP() AS index_built_at
FROM base_profile b
LEFT JOIN hotel_agg h ON b.city = h.nearest_city
CROSS JOIN weather_score ws ON b.city = ws.city
CROSS JOIN max_landmarks ml
CROSS JOIN max_restaurants mr
CROSS JOIN max_hotel_value mv
