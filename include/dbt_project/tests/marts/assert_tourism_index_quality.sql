SELECT city
FROM {{ ref('mart_city_tourism_index') }}
WHERE tourism_index < 0
   OR tourism_index > 100
   OR data_coverage_pct < 0
   OR data_coverage_pct > 100
   OR index_methodology_version IS NULL
