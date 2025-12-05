{{ config(materialized='view') }}

with src as (
    select airline_id, name, alias, iata, icao, callsign, country, active
    from {{ source('raw','AIRLINES_RAW') }}
)

select
    try_to_number(airline_id)             as airline_id,
    nullif(name, '')                      as airline_name,
    nullif(replace(alias, '\N', ''), '')  as alias,
    NULLIF(NULLIF(TRIM(IATA), ''), '-') AS iata,
    NULLIF(NULLIF(TRIM(ICAO), ''), '/A') AS icao,
    nullif(replace(callsign, '\N', ''), '') as callsign,
    nullif(replace(country, '\N', ''), '')  as country,
    upper(nullif(active, ''))               as is_active
from src
where airline_id is not null
