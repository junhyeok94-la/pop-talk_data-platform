select service_system,service_movie_id from {{ ref('movie_service_map') }}
group by service_system,service_movie_id having count(*)<>1
