{{ config(materialized='table') }}

/*
  Gold layer: Hotel analysis — price vs rating, amenities richness, and
  weather-adjusted recommendation scores.

  Joins hotels to their nearest city (via city_name_mapping on name proximity
  or coordinates) and enriches with the latest daily weather for context.
*/

WITH hotels AS (
    SELECT * FROM {{ ref('stg_hotel_data') }}
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

-- Approximate nearest city by coordinate distance (great-circle, km)
hotel_with_city AS (
    SELECT
        h.*,
        m.city_name_normalized AS nearest_city,
        -- Haversine-ish distance using degree deltas (good enough for ranking)
        SQRT(
            POWER(h.latitude - m_lat.latitude, 2) +
            POWER(h.longitude - m_lat.longitude, 2)
        ) * 111.0 AS approx_distance_km
    FROM hotels h
    CROSS JOIN LATERAL (
        SELECT m.city_name_normalized,
               w.latitude AS latitude,
               w.longitude AS longitude
        FROM {{ ref('stg_daily_weather_data') }} w
        JOIN {{ ref('city_name_mapping') }} m ON TRIM(w.city) = TRIM(m.city_name_arabic)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY m.city_name_normalized
            ORDER BY w.ingested_at DESC
        ) = 1
    ) m_lat
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY h.hotel_id
        ORDER BY approx_distance_km ASC
    ) = 1
),

enriched AS (
    SELECT
        h.hotel_id,
        h.hotel_name,
        h.type,
        h.nearest_city,
        h.approx_distance_km,
        h.final_price,
        h.base_price,
        h.total_taxes,
        h.weighted_score,
        h.overall_rating,
        h.location_rating,
        h.recommended_rank,
        h.amenities,
        h.check_in_time,
        h.check_out_time,
        -- Value-for-money score: higher rating per dollar
        CASE
            WHEN h.final_price > 0 THEN h.weighted_score / h.final_price
            ELSE 0
        END AS value_score,
        -- Tax burden percentage
        CASE
            WHEN h.base_price > 0 THEN (h.total_taxes / h.base_price) * 100
            ELSE 0
        END AS tax_pct,
        -- Price tier
        CASE
            WHEN h.final_price = 0 THEN 'Unknown'
            WHEN h.final_price < 1000 THEN 'Budget'
            WHEN h.final_price < 3000 THEN 'Mid-range'
            WHEN h.final_price < 6000 THEN 'Upper-mid'
            ELSE 'Luxury'
        END AS price_tier,
        -- Amenities count (rough)
        ARRAY_SIZE(SPLIT(h.amenities, '-')) AS amenity_count,
        d.max_temp AS weather_max_temp_c,
        d.min_temp AS weather_min_temp_c
    FROM hotel_with_city h
    LEFT JOIN latest_daily d ON h.nearest_city = d.city
)

SELECT *
FROM enriched
