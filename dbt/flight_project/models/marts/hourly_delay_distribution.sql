{{ config(schema='marts', materialized='table') }}

with f as (
    select *
    from {{ ref('fact_flights') }}
)


select
    dep_hour,
    count(*) as flights,
    avg(dep_delay) as avg_dep_delay,
    avg(arr_delay) as avg_arr_delay
from f
group by dep_hour
order by dep_hour
