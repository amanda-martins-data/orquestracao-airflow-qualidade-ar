-- air_quality_daily
-- Mart analítico: uma linha por (cidade, poluente, dia), pronta para
-- ser consumida por um dashboard (Power BI / Looker / etc).

with stg as (
    select * from {{ ref('stg_air_quality') }}
)

select
    city,
    parameter,
    unit,
    measured_date,
    count(*)              as reading_count,
    round(avg(value), 2)  as avg_value,
    round(min(value), 2)  as min_value,
    round(max(value), 2)  as max_value
from stg
group by 1, 2, 3, 4
order by measured_date desc, city, parameter
