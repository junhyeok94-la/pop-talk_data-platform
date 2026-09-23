select f.boxoffice_key
from {{ ref('fct_boxoffice_daily') }} f
left join {{ ref('mart_boxoffice_daily') }} m on f.boxoffice_key = m.boxoffice_key
where m.boxoffice_key is null

union all

select m.boxoffice_key
from {{ ref('mart_boxoffice_daily') }} m
left join {{ ref('fct_boxoffice_daily') }} f on m.boxoffice_key = f.boxoffice_key
where f.boxoffice_key is null
