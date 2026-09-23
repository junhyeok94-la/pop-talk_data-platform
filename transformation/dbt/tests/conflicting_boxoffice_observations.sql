with observations as (
    select *, max(source_observed_at) over (partition by kofic_movie_cd,target_date) as latest_at
    from {{ source('stg','boxoffice_observations') }}
)
select kofic_movie_cd,target_date,source_observed_at from observations
where source_observed_at=latest_at
group by kofic_movie_cd,target_date,source_observed_at
having count(distinct (rank,audience_count,audience_accumulated,sales_amount))>1
