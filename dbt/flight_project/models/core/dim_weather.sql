{{ config(
    schema='core',
    materialized='table'
) }}

with src as (
    select
        station,
        observation_ts,
        observation_hour,
        latitude,
        longitude,
        elevation,
        name as station_name,
        wind_direction_deg,
        wind_speed_mps,
        wind_gust_mps,
        temperature_c,
        dew_point_c,
        sea_level_pressure_pa,
        visibility_m,
        ceiling_ft
    from {{ ref('stg_weather') }}
),

final as (
    select
        station,
        observation_ts,
        observation_hour,
        latitude,
        longitude,
        elevation,
        station_name,
        temperature_c,
        dew_point_c,

        case 
            when sea_level_pressure_pa is null then null
            else sea_level_pressure_pa / 10.0 
        end as pressure_hpa,

        visibility_m,
        ceiling_ft as cloud_ceiling_ft,

        wind_direction_deg,
        wind_speed_mps,
        wind_gust_mps
    from src
)

select
    {{ dbt_utils.generate_surrogate_key(['station','observation_ts']) }} as weather_sk,
    station,
    observation_ts,
    observation_hour,
    latitude,
    longitude,
    elevation,
    station_name,
    temperature_c,
    dew_point_c,
    pressure_hpa,
    visibility_m,
    cloud_ceiling_ft,
    wind_direction_deg,
    wind_speed_mps,
    wind_gust_mps

from final
