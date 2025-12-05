{{ config(schema='marts_jfk', materialized='table') }}

with f as (
    select *
    from {{ ref('fact_flights') }}
    where (upper(origin) = 'JFK' or upper(dest) = 'JFK')
     and dep_time is not null        -- remove null departure time
     
)

select
    date_trunc('day', dep_time) as flight_day,
    count(*) as total_flights,
    avg(arr_delay) as avg_arr_delay,
    avg(dep_delay) as avg_dep_delay,
    avg(temperature_c) as avg_temp_c,
    avg(wind_speed_mps) as avg_wind_speed,
    avg(visibility_m) as avg_visibility_m,
    avg(pressure_hpa) as avg_pressure
from f
group by 1
order by 1