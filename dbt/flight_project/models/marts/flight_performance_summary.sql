{{ config(schema='marts', materialized='table') }}

with f as (
    select *
    from {{ ref('fact_flights') }}
)

select
    origin,
    dest,
    count(*) as total_flights,
    avg(dep_delay) as avg_dep_delay,
    avg(arr_delay) as avg_arr_delay,
    avg(distance) as avg_distance,
    sum(case when arr_delay > 15 then 1 else 0 end) * 100.0 / count(*) as pct_delayed_arr,
    sum(case when dep_delay > 15 then 1 else 0 end) * 100.0 / count(*) as pct_delayed_dep
from f
group by 1, 2
order by total_flights desc
