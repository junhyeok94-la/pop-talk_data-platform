with ranked as (
    select s.*, l.source_run_id,
        row_number() over (partition by kofic_movie_cd, target_date
            order by source_observed_at desc, l.loaded_at desc, s.load_id desc) as rn
    from {{ source('stg', 'boxoffice_observations') }} s
    join {{ source('ops', 'source_loads') }} l using (load_id)
)
select m.movie_key, b.kofic_movie_cd, b.target_date, b.rank,
    b.audience_count, b.audience_accumulated, b.sales_amount,
    b.source_observed_at, b.source_run_id, b.load_id
from ranked b
left join {{ ref('dim_movie') }} m using (kofic_movie_cd)
where b.rn=1
