{{ config(schema='marts_jfk', materialized='table') }}

with f as (
    select *
    from {{ ref('fact_flights') }}
    where upper(origin) = 'JFK' or upper(dest) = 'JFK'
)

select
    dep_hour,
    count(*) as flights,
    avg(dep_delay) as avg_dep_delay,
    avg(arr_delay) as avg_arr_delay
from f
group by dep_hour
order by dep_hour