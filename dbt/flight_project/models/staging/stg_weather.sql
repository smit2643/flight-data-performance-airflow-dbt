{{ config(materialized='view') }}

with src_raw as (
    select
        station,
        date as observation_ts,
        date_trunc('hour', date) as observation_hour,

        source,
        latitude,
        longitude,
        elevation,
        name,
        report_type,

        /* Fix call sign */
        case 
            when call_sign = '99999' then 'KJFK'
            else call_sign
        end as call_sign,

        quality_control,

        cast(wnd as varchar) as wnd_raw,
        cast(cig as varchar) as cig_raw,
        cast(vis as varchar) as vis_raw,
        cast(tmp as varchar) as tmp_raw,
        cast(dew as varchar) as dew_raw,
        cast(slp as varchar) as slp_raw
    from {{ source('raw','WEATHER_RAW') }}
),


cleaned as (
    select
        station,
        observation_ts,
        observation_hour,

        source,
        latitude,
        longitude,
        elevation,
        name,
        report_type,
        call_sign,
        quality_control,
        split_part(wnd_raw, ',', 1) as wind_dir_raw,
        split_part(wnd_raw, ',', 2) as wind_speed_raw,
        split_part(wnd_raw, ',', 4) as wind_gust_raw,
        split_part(cig_raw, ',', 1) as cig_height_raw,

        split_part(vis_raw, ',', 1) as visibility_raw,
        split_part(tmp_raw, ',', 1) as temp_raw,
        split_part(dew_raw, ',', 1) as dew_raw2,
        split_part(slp_raw, ',', 1) as slp_raw2
    from src_raw
),

final_clean as (
    select
        station,
        observation_ts,
        observation_hour,
        source,
        latitude,
        longitude,
        elevation,
        name,
        report_type,
        call_sign,
        quality_control,
        nullif(wind_dir_raw, '999')::int as wind_direction_deg,
        nullif(wind_speed_raw, '9')::int as wind_speed_mps,
        nullif(wind_gust_raw, '9999')::int as wind_gust_mps,
        nullif(cig_height_raw, '99999')::int as ceiling_ft,
        nullif(visibility_raw, '999999')::int as visibility_m,
        nullif(replace(temp_raw, '+',''), '9999')::int as temperature_c,
        nullif(replace(dew_raw2, '+',''), '9999')::int as dew_point_c,
        nullif(slp_raw2, '99999')::int as sea_level_pressure_pa
    from cleaned
)

select * from final_clean
