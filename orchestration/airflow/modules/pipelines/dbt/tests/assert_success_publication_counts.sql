select exchange_ready_key
from {{ ref('mart_data_quality') }}
where movie_count_difference <> 0 or boxoffice_count_difference <> 0
