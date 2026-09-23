with movie_quality as (
  select source_run_id, artifact_version,
    count(*) as observed_movie_count,
    count_if(policy_eligible) as eligible_movie_count,
    count_if(not policy_eligible) as excluded_movie_count,
    count_if(kmdb_mapping_status = 'MATCHED') as kmdb_matched_count,
    count_if(kmdb_mapping_status = 'REVIEW_REQUIRED') as kmdb_review_required_count,
    count_if(title_ko is null) as missing_title_count,
    count_if(release_date is null) as missing_release_date_count
  from {{ ref('stg_movie_observations') }} group by 1, 2
), boxoffice_quality as (
  select source_run_id, artifact_version, count(*) as observed_boxoffice_count
  from {{ ref('stg_boxoffice_observations') }} group by 1, 2
)
select
    p.source_run_id, p.artifact_version, p.publication_revision,
    p.exchange_ready_key, p.completed_at,
    p.movie_count as ledger_movie_count,
    coalesce(m.observed_movie_count, 0) as observed_movie_count,
    p.boxoffice_count as ledger_boxoffice_count,
    coalesce(b.observed_boxoffice_count, 0) as observed_boxoffice_count,
    coalesce(m.eligible_movie_count, 0) as eligible_movie_count,
    coalesce(m.excluded_movie_count, 0) as excluded_movie_count,
    coalesce(m.kmdb_matched_count, 0) as kmdb_matched_count,
    coalesce(m.kmdb_review_required_count, 0) as kmdb_review_required_count,
    coalesce(m.missing_title_count, 0) as missing_title_count,
    coalesce(m.missing_release_date_count, 0) as missing_release_date_count,
    p.movie_count - coalesce(m.observed_movie_count, 0) as movie_count_difference,
    p.boxoffice_count - coalesce(b.observed_boxoffice_count, 0) as boxoffice_count_difference,
    (select count(*) from {{ ref('fct_boxoffice_daily') }} f
      left join {{ ref('dim_movie') }} d on f.movie_key = d.movie_key
      where d.movie_key is null) as current_unmatched_boxoffice_movie_count
from {{ ref('stg_successful_exchange_publications') }} p
left join movie_quality m
  on p.source_run_id = m.source_run_id and p.artifact_version = m.artifact_version
left join boxoffice_quality b
  on p.source_run_id = b.source_run_id and p.artifact_version = b.artifact_version
