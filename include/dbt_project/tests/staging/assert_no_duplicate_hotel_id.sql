-- Fails if more than one hotel row shares the same hotel_id (dedup grain).
select hotel_id, count(*) as row_count
from {{ ref('mart_hotel_analysis') }}
group by 1
having count(*) > 1
