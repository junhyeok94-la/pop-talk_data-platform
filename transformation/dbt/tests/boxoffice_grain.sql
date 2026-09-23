select kofic_movie_cd,target_date from {{ ref('fct_boxoffice_daily') }}
group by kofic_movie_cd,target_date having count(*)<>1
