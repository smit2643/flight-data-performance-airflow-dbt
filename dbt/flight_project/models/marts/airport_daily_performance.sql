{{ config(schema='marts', materialized='table') }}

with f as (
    select *
    from {{ ref('fact_flights') }}
     where dep_time is not null 
)

select
    origin as airport,
    date_trunc('day', dep_time) as flight_day,
    count(*) as total_flights,
    avg(dep_delay) as avg_dep_delay,
    avg(arr_delay) as avg_arr_delay,
    sum(case when arr_delay > 15 then 1 else 0 end) as delayed_arrivals,
    sum(case when dep_delay > 15 then 1 else 0 end) as delayed_departures
from f
group by 1, 2
order by 1, 2
