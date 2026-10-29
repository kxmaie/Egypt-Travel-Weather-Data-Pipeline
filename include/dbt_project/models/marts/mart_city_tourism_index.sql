{{ config(materialized='table') }}

/*
  Weighted destination score. Component weights are renormalized over the
  components with data, so a missing source is not treated as a poor result.
  Count and hotel-value components are relative to the current city cohort;
  data_coverage_pct indicates how much of the score could be evaluated.
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

max_components AS (
    SELECT
        MAX(landmark_count) AS max_landmarks,
        MAX(restaurant_variety) AS max_restaurant_variety,
        MAX(avg_value_score) AS max_hotel_value
    FROM base_profile
    LEFT JOIN hotel_agg ON base_profile.city = hotel_agg.nearest_city
),

scores AS (
    SELECT
        b.*,
        COALESCE(h.hotel_count, 0) AS hotel_count,
        ROUND(COALESCE(h.avg_price_per_night, 0), 2) AS avg_hotel_price_per_night,
        ROUND(COALESCE(h.avg_hotel_score, 0), 2) AS avg_hotel_score,
        CASE
            WHEN b.current_temp_c IS NULL THEN NULL
            WHEN b.current_temp_c BETWEEN 18 AND 26 THEN 100
            WHEN b.current_temp_c BETWEEN 12 AND 18 OR b.current_temp_c BETWEEN 26 AND 32 THEN 75
            WHEN b.current_temp_c BETWEEN 5 AND 12 OR b.current_temp_c BETWEEN 32 AND 38 THEN 50
            ELSE 25
        END AS weather_comfort_score,
        CASE
            WHEN b.max_wind_speed IS NULL THEN NULL
            WHEN b.max_wind_speed < 20 THEN 100
            WHEN b.max_wind_speed < 40 THEN 75
            WHEN b.max_wind_speed < 60 THEN 50
            ELSE 25
        END AS wind_score,
        CASE WHEN b.landmark_data_available AND c.max_landmarks > 0
            THEN b.landmark_count * 100.0 / c.max_landmarks
        END AS landmark_score,
        CASE WHEN b.restaurant_data_available AND c.max_restaurant_variety > 0
            THEN b.restaurant_variety * 100.0 / c.max_restaurant_variety
        END AS restaurant_score,
        CASE WHEN c.max_hotel_value > 0 AND h.avg_value_score IS NOT NULL
            THEN h.avg_value_score * 100.0 / c.max_hotel_value
        END AS hotel_value_score,
        h.avg_value_score
    FROM base_profile b
    LEFT JOIN hotel_agg h ON b.city = h.nearest_city
    CROSS JOIN max_components c
),

weighted AS (
    SELECT
        *,
          COALESCE(weather_comfort_score * 0.35, 0)
        + COALESCE(wind_score * 0.15, 0)
        + COALESCE(landmark_score * 0.25, 0)
        + COALESCE(restaurant_score * 0.15, 0)
        + COALESCE(hotel_value_score * 0.10, 0) AS weighted_points,
          CASE WHEN weather_comfort_score IS NOT NULL THEN 0.35 ELSE 0 END
        + CASE WHEN wind_score IS NOT NULL THEN 0.15 ELSE 0 END
        + CASE WHEN landmark_score IS NOT NULL THEN 0.25 ELSE 0 END
        + CASE WHEN restaurant_score IS NOT NULL THEN 0.15 ELSE 0 END
        + CASE WHEN hotel_value_score IS NOT NULL THEN 0.10 ELSE 0 END AS available_weight
    FROM scores
),

final_scores AS (
    SELECT
        *,
        weighted_points / NULLIF(available_weight, 0) AS tourism_index_raw
    FROM weighted
)

SELECT
    city,
    daily_max_temp_c,
    daily_min_temp_c,
    current_temp_c,
    current_humidity_pct,
    landmark_count,
    landmark_data_available,
    restaurant_count,
    restaurant_variety,
    restaurant_data_available,
    weather_data_available,
    hotel_count,
    avg_hotel_price_per_night,
    avg_hotel_score,
    ROUND(weather_comfort_score, 1) AS weather_comfort_score,
    ROUND(wind_score, 1) AS wind_score,
    ROUND(landmark_score, 1) AS landmark_score,
    ROUND(restaurant_score, 1) AS restaurant_score,
    ROUND(hotel_value_score, 1) AS hotel_value_score,
    ROUND(tourism_index_raw, 1) AS tourism_index,
    ROUND(available_weight * 100, 1) AS data_coverage_pct,
    'v2' AS index_methodology_version,
    CASE
        WHEN tourism_index_raw IS NULL THEN 'Insufficient Data'
        WHEN tourism_index_raw >= 75 THEN 'Top Destination'
        WHEN tourism_index_raw >= 50 THEN 'Good Destination'
        ELSE 'Emerging Destination'
    END AS destination_tier,
    CURRENT_TIMESTAMP() AS index_built_at
FROM final_scores
