-- A tie with different business content must not be resolved by arrival order.
with observations as (
    select *, max(source_observed_at) over (partition by kofic_movie_cd) as latest_at
    from {{ source('stg','movie_observations') }}
)
select kofic_movie_cd,source_observed_at from observations
where source_observed_at=latest_at
group by kofic_movie_cd,source_observed_at
having count(distinct (content_sha256,attributes->>'policy_sha256',attributes->>'matching_sha256'))>1
