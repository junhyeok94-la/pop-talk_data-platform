-- Full snapshots also propagate hard deletions; older backfills never resurrect reviews.
with latest as (
    select load_id from {{ source('ops', 'source_loads') }}
    where source_kind='service_reviews'
    order by observed_at desc, loaded_at desc, load_id desc limit 1
)
select r.*, m.movie_key,
    (r.deleted_at is null and r.status='ACTIVE') as is_public
from {{ source('stg', 'review_observations') }} r
join latest using (load_id)
left join {{ ref('movie_service_map') }} m using (service_system, service_movie_id)
