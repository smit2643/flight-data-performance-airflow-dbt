{{ config(schema='marts_jfk', materialized='table') }}

with f as (
    select *
    from {{ ref('fact_flights') }}
    where (upper(origin) = 'JFK' or upper(dest) = 'JFK')
     and dep_time is not null       
     
)

select
    marketing_iata as airline,
    count(*) as flights,
    avg(arr_delay) as avg_arr_delay,
    100.0 * sum(case when arr_delay > 15 then 1 else 0 end) / nullif(count(*),0) as pct_late,
    rank() over (order by avg(arr_delay)) as on_time_rank
from f
group by 1
order by avg_arr_delay