with ranked as (
    select *, row_number() over (
        partition by target_date, kofic_movie_cd
        order by {{ observation_recency_order() }}
    ) as recency_rank
    from {{ ref('stg_boxoffice_observations') }}
)
select
    md5(target_date::string || '|' || kofic_movie_cd) as boxoffice_key,
    md5('kofic:' || kofic_movie_cd) as movie_key,
    target_date, kofic_movie_cd, rank, sales_amount, audience_count,
    audience_accumulated, source_run_id, artifact_version, publication_revision,
    exchange_ready_key, source_object_key, source_observed_at,
    publication_completed_at, record_sha256, observation_sha256
from ranked where recency_rank = 1
