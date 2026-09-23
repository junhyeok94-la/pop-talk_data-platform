select
    o.source_run_id || '|' || o.artifact_version || '|' || o.target_date::string || '|' || o.kofic_movie_cd as observation_key,
    o.source_run_id, o.artifact_version, p.publication_revision,
    p.exchange_ready_key, p.completed_at as publication_completed_at,
    o.target_date, o.kofic_movie_cd, o.source_observed_at, o.record_sha256, o.loaded_at,
    o.payload:rank::integer as rank,
    o.payload:sales_amount::number(38,0) as sales_amount,
    o.payload:audience_count::number(38,0) as audience_count,
    o.payload:audience_accumulated::number(38,0) as audience_accumulated,
    nullif(trim(o.payload:source_object_key::string), '') as source_object_key,
    o.payload:observation_sha256::string as observation_sha256
from {{ source('pop_talk_staging', 'boxoffice_observations_raw') }} o
join {{ ref('stg_successful_exchange_publications') }} p
  on o.source_run_id = p.source_run_id and o.artifact_version = p.artifact_version
