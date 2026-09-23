select b.*, m.title_ko, m.release_date, m.policy_eligible
from {{ ref('fct_boxoffice_daily') }} b
left join {{ ref('dim_movie') }} m using (movie_key)
