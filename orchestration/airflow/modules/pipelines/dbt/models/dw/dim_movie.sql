with ranked as (
    select *, row_number() over (
        partition by canonical_movie_key
        order by {{ observation_recency_order() }}
    ) as recency_rank
    from {{ ref('stg_movie_observations') }}
)
select
    md5(canonical_movie_key) as movie_key, canonical_movie_key,
    kofic_movie_cd, kmdb_id, kmdb_matched, kmdb_mapping_status,
    provisional_kmdb_id, kmdb_candidate_count, kmdb_candidates_truncated,
    kmdb_candidates_ambiguous, title_ko, title_en, title_original, release_date,
    production_year, runtime_minutes, movie_type, production_status, countries,
    genres, directors, director_names_en, actors, actor_roles, production_companies,
    viewing_grade, poster_url, plot, policy_eligible, policy_exclusion_reasons,
    policy_version, ingestion_source, source_object_key, source_run_id,
    artifact_version, publication_revision, exchange_ready_key, source_observed_at,
    publication_completed_at, record_sha256, content_sha256, policy_sha256, matching_sha256
from ranked where recency_rank = 1
