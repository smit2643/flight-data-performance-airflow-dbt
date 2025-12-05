{{ config(
    schema='core',
    materialized='table'
) }}

with bts as (
    select *,  date_trunc('hour', dep_time) as dep_hour
    from {{ ref('stg_bts') }}
),

origin_airport as (
    select
        iata_code               as origin_iata,
        airport_sk              as origin_airport_sk,
        airport_name            as origin_airport_name,
        city                    as origin_airport_city,
        country                 as origin_airport_country,
        latitude                as origin_airport_latitude,
        longitude               as origin_airport_longitude
    from {{ ref('dim_airports') }}
),

dest_airport as (
    select
        iata_code               as dest_iata,
        airport_sk              as dest_airport_sk,
        airport_name            as dest_airport_name,
        city                    as dest_airport_city,
        country                 as dest_airport_country,
        latitude                as dest_airport_latitude,
        longitude               as dest_airport_longitude
    from {{ ref('dim_airports') }}
),

joined_airports as (
    select
        bts.*,
        oa.origin_airport_sk,
        oa.origin_airport_name,
        oa.origin_airport_city,
        oa.origin_airport_country,
        oa.origin_airport_latitude,
        oa.origin_airport_longitude,
        da.dest_airport_sk,
        da.dest_airport_name,
        da.dest_airport_city,
        da.dest_airport_country,
        da.dest_airport_latitude,
        da.dest_airport_longitude
    from bts
    left join origin_airport oa
        on upper(bts.origin) = upper(oa.origin_iata)
    left join dest_airport da
        on upper(bts.dest) = upper(da.dest_iata)
),

weather as (
    select *
    from {{ ref('dim_weather') }}
),

weather_join as (
    select
        f.*,
        w.weather_sk,
        w.temperature_c,
        w.wind_speed_mps,        
        w.wind_gust_mps,         
        w.wind_direction_deg,
        w.visibility_m,
        w.pressure_hpa
    from joined_airports f
    left join weather w
        on f.dep_hour = w.observation_hour
)

select
    {{ dbt_utils.generate_surrogate_key([
        'flight_date',
        'marketing_iata',
        'marketing_flight_number',
        'dep_time'
    ]) }} as flight_sk,
    *
from weather_join
