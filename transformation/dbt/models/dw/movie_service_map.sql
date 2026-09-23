-- Only reviewed service movies are needed in Phase 1. Never change service IDs.
with latest as (
    select load_id from {{ source('ops', 'source_loads') }}
    where source_kind='service_reviews'
    order by observed_at desc, loaded_at desc, load_id desc limit 1
)
select distinct r.service_system, r.service_movie_id, r.kofic_movie_cd, m.movie_key
from {{ source('stg', 'review_observations') }} r
join latest using (load_id)
left join {{ ref('dim_movie') }} m using (kofic_movie_cd)
