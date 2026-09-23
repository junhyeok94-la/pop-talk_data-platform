with dates as (
    select target_date as calendar_date from {{ ref('fct_boxoffice_daily') }}
    union select release_date from {{ ref('dim_movie') }} where release_date is not null
), bounds as (
    select min(calendar_date) as first_date, max(calendar_date) as last_date from dates
)
select d::date as calendar_date, extract(year from d)::integer as year,
    extract(month from d)::integer as month, extract(day from d)::integer as day,
    extract(isodow from d)::integer as iso_weekday
from bounds, lateral generate_series(first_date, last_date, interval '1 day') d
