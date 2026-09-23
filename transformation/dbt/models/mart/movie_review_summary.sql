select movie_key, source_system,
    count(*) filter (where is_public) as review_count,
    avg(rating) filter (where is_public) as average_rating,
    max(source_observed_at) as review_as_of
from {{ ref('fct_review') }}
group by movie_key, source_system
