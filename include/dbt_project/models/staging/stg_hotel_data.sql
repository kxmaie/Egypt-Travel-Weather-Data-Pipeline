{{config(schema='silver_layer')}}

WITH raw_data AS(
    SELECT * FROM {{ source('raw_data','Bronze_hotel_data')}}
),

cleaned_data AS(
   SELECT 
        CAST(id as integer) as hotel_id,
        TRIM(type) as type,
        TRIM(name) as hotel_name,
        COALESCE(link,'unknown') as link_for_the_hotel,
        latitude,
        longitude,
        COALESCE(check_in_time,'unknown') as check_in_time,
        COALESCE(check_out_time,'unknown') as check_out_time,
        COALESCE(NULLIF(reviews, 0) * NULLIF(overall_rating, 0), 0) as weighted_score,
        COALESCE (TRIM(replace(amenities,',','-')),'unknown') as amenities,
        COALESCE(TRIM(replace(excluded_amenities,',','-')),'unknown') as excluded_amenities,
        TRIM(split_part(nearby_places,':',1)) as nearby_places,
        COALESCE( CAST(REGEXP_REPLACE(rate_per_night_lowest, '[^0-9.]', '') AS FLOAT),0) AS final_price,
        COALESCE( CAST(REGEXP_REPLACE(rate_per_night_before_taxes_and_fees, '[^0-9.]', '') AS FLOAT),0) AS base_price,
        TO_TIMESTAMP(ingested_at / 1000000000) as ingested_at
    FROM raw_data


)
SELECT * , 
      COALESCE ( (final_price - base_price),0) AS total_taxes,
       ROW_NUMBER() OVER (ORDER BY weighted_score DESC, hotel_id ASC) AS recommended_rank
FROM cleaned_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY hotel_id ORDER BY ingested_at DESC) = 1


