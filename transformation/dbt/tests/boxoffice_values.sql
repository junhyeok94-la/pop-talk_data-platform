select * from {{ ref('fct_boxoffice_daily') }}
where rank<=0 or audience_count<0 or audience_accumulated<0 or sales_amount<0
