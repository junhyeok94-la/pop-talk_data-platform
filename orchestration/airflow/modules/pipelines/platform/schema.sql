CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS ops.source_loads (
    load_id text PRIMARY KEY,
    source_kind text NOT NULL CHECK (source_kind IN ('movie_daily','movie_initial','legacy_snapshot','service_reviews')),
    source_uri text NOT NULL,
    loader_version text NOT NULL,
    input_sha256 text NOT NULL CHECK (length(input_sha256)=64),
    source_run_id text NOT NULL,
    observed_at timestamptz,
    loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    quality jsonb NOT NULL,
    UNIQUE (source_uri, loader_version)
);
CREATE TABLE IF NOT EXISTS stg.movie_observations (
    load_id text NOT NULL REFERENCES ops.source_loads,
    kofic_movie_cd text NOT NULL,
    source_object_key text NOT NULL,
    source_observed_at timestamptz,
    content_sha256 text NOT NULL,
    attributes jsonb NOT NULL,
    PRIMARY KEY (load_id, kofic_movie_cd)
);
CREATE TABLE IF NOT EXISTS stg.boxoffice_observations (
    load_id text NOT NULL REFERENCES ops.source_loads,
    kofic_movie_cd text NOT NULL,
    target_date date NOT NULL,
    rank integer NOT NULL CHECK (rank>0),
    audience_count bigint NOT NULL CHECK (audience_count>=0),
    audience_accumulated bigint NOT NULL CHECK (audience_accumulated>=0),
    sales_amount bigint NOT NULL CHECK (sales_amount>=0),
    source_object_key text NOT NULL,
    source_observed_at timestamptz NOT NULL,
    observation_sha256 text NOT NULL,
    PRIMARY KEY (load_id, kofic_movie_cd, target_date)
);
CREATE TABLE IF NOT EXISTS stg.review_observations (
    load_id text NOT NULL REFERENCES ops.source_loads,
    service_system text NOT NULL,
    review_id uuid NOT NULL,
    service_movie_id bigint NOT NULL,
    kofic_movie_cd text,
    source_system text NOT NULL,
    source_review_key text,
    rating numeric(2,1) NOT NULL CHECK (rating BETWEEN 0.5 AND 5 AND mod(rating,0.5)=0),
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    deleted_at timestamptz,
    original_reviewed_at timestamptz,
    source_observed_at timestamptz NOT NULL,
    PRIMARY KEY (load_id, service_system, review_id)
);
CREATE TABLE IF NOT EXISTS ops.service_publications (
    publication_id text PRIMARY KEY,
    cutoff timestamptz NOT NULL UNIQUE,
    model_version text NOT NULL,
    input_sha256 text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING','SUCCEEDED','FAILED')),
    result jsonb,
    error_type text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz
);
