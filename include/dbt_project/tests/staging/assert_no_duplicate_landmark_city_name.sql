-- Fails if more than one row exists for the same city + landmark name.
select
    city,
    trim(landmark_name) as landmark_name,
    count(*) as row_count
from {{ ref('stg_famous_landmark_data') }}
group by 1, 2
having count(*) > 1
