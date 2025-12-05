{{ config(schema='marts_jfk', materialized='table') }}

with f as (
    select *
    from {{ ref('fact_flights') }}
    where (upper(origin) = 'JFK' or upper(dest) = 'JFK')
      and dep_time is not null
)

select
    case
      when upper(origin) = 'JFK' then origin
      when upper(dest)   = 'JFK' then dest
      else null
    end as airport,

    date_trunc('day', dep_time) as flight_day,
    count(*) as total_flights,
    avg(dep_delay) as avg_dep_delay,
    avg(arr_delay) as avg_arr_delay,
    sum(case when arr_delay > 15 then 1 else 0 end) as delayed_arrivals,
    sum(case when dep_delay > 15 then 1 else 0 end) as delayed_departures
from f
group by 1, 2
order by 1, 2