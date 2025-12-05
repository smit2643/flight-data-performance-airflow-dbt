{{ config(schema='marts_jfk', materialized='table') }}

with f as (
    select *
    from {{ ref('fact_flights') }}
    where upper(origin) = 'JFK' or upper(dest) = 'JFK'
)

select
    origin,
    dest,
    count(*) as total_flights,
    avg(dep_delay) as avg_dep_delay,
    avg(arr_delay) as avg_arr_delay,
    avg(distance) as avg_distance,
    100.0 * sum(case when arr_delay > 15 then 1 else 0 end) / nullif(count(*),0) as pct_delayed_arr,
    100.0 * sum(case when dep_delay > 15 then 1 else 0 end) / nullif(count(*),0) as pct_delayed_dep
from f
group by 1, 2
order by total_flights desc