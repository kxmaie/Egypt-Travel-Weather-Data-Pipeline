SELECT
    restaurant_source_id,
    city,
    COUNT(*) AS row_count
FROM {{ ref('stg_restaurant_data') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
