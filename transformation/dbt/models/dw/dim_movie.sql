-- Late-arriving historical loads cannot replace a newer source observation.
with ranked as (
    select s.*, l.source_run_id,
        row_number() over (partition by kofic_movie_cd
            order by source_observed_at desc nulls last, l.loaded_at desc, s.load_id desc) as rn
    from {{ source('stg', 'movie_observations') }} s
    join {{ source('ops', 'source_loads') }} l using (load_id)
)
select md5('kofic:' || kofic_movie_cd) as movie_key, kofic_movie_cd,
    attributes ->> 'title_ko' as title_ko,
    nullif(attributes ->> 'release_date', '')::date as release_date,
    (attributes ->> 'policy_eligible')::boolean as policy_eligible,
    attributes, content_sha256, source_observed_at, source_run_id, load_id
from ranked where rn=1
