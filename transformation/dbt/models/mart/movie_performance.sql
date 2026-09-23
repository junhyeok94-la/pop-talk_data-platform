with totals as (
    select movie_key, sum(audience_count) as observed_daily_audience,
        sum(sales_amount) as observed_daily_sales, count(*) as observed_boxoffice_days,
        max(source_observed_at) as boxoffice_as_of
    from {{ ref('fct_boxoffice_daily') }} group by movie_key
), latest as (
    select distinct on (movie_key) movie_key, target_date, audience_accumulated
    from {{ ref('fct_boxoffice_daily') }} order by movie_key, target_date desc
)
select m.movie_key, m.title_ko, r.source_system as review_source,
    r.review_count, r.average_rating, r.review_as_of,
    b.observed_daily_audience, b.observed_daily_sales, b.observed_boxoffice_days,
    b.boxoffice_as_of, l.target_date as latest_boxoffice_date,
    l.audience_accumulated as latest_audience_accumulated
from {{ ref('dim_movie') }} m
left join totals b using (movie_key)
left join latest l using (movie_key)
left join {{ ref('movie_review_summary') }} r using (movie_key)
