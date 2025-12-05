{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('raw','BTS_RAW') }}
)

select
    year,
    quarter,
    month,
    day_of_month,
    day_of_week,
    flight_date,
    nullif(marketing_airline_network,'') as marketing_airline,
    nullif(iata_code_marketing_airline,'') as marketing_iata,
    nullif(flight_number_marketing_airline,'') as marketing_flight_number,
    nullif(operating_airline,'') as operating_airline,
    nullif(iata_code_operating_airline,'') as operating_iata,
    nullif(flight_number_operating_airline,'') as operating_flight_number,
    nullif(origin,'') as origin,
    nullif(origin_city_name,'') as origin_city,
    nullif(origin_state_name,'') as origin_state,
    nullif(dest,'') as dest,
    nullif(dest_city_name,'') as dest_city,
    nullif(dest_state_name,'') as dest_state,
    distance,
    distance_group,
    nullif(crs_dep_time,'') as crs_dep_time_raw,
    nullif(dep_time,'') as dep_time_raw,
    nullif(crs_arr_time,'') as crs_arr_time_raw,
    nullif(arr_time,'') as arr_time_raw,
    {{ convert_to_timestamp("FLIGHT_DATE","CRS_DEP_TIME") }} AS crs_dep_time,
    {{ convert_to_timestamp("FLIGHT_DATE","DEP_TIME") }} AS dep_time,
    {{ convert_to_timestamp("FLIGHT_DATE","CRS_ARR_TIME") }} AS crs_arr_time,
    {{ convert_to_timestamp("FLIGHT_DATE","ARR_TIME") }} AS arr_time,
    dep_delay,
    arr_delay
from src
where flight_date is not null

 