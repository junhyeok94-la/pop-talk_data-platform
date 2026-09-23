select
    f.boxoffice_key, f.movie_key, m.movie_key is not null as movie_dimension_matched,
    m.policy_eligible, m.title_ko, m.release_date, m.genres,
    f.target_date, f.rank, f.sales_amount, f.audience_count,
    f.audience_accumulated, f.source_observed_at, f.source_run_id,
    f.publication_revision
from {{ ref('fct_boxoffice_daily') }} f
left join {{ ref('dim_movie') }} m on f.movie_key = m.movie_key
