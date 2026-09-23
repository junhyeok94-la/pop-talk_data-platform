with successful as (
    select *, row_number() over (
        partition by source_run_id, artifact_version
        order by publication_revision desc, completed_at desc, exchange_ready_key desc
    ) as publication_rank
    from {{ source('pop_talk_staging', 'silver_exchange_loads') }}
    where status = 'SUCCESS'
)
select
    source_run_id || '|' || artifact_version as publication_key,
    source_run_id, artifact_version, publication_revision, exchange_ready_key,
    source_bundle_sha256, movie_count, boxoffice_count, completed_at
from successful where publication_rank = 1
