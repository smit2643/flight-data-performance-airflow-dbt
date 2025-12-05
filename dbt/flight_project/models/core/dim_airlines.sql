{{ config(schema='core', materialized='table') }}

with src as (
    select *
    from {{ ref('stg_airlines') }}
),

cleaned as (
    select
        airline_id,
        nullif(trim(airline_name), '') as airline_name,
        nullif(trim(alias), '') as alias,
        nullif(trim(iata), '') as iata,
        nullif(nullif(trim(icao), ''),'N/A') as icao,
        case 
            when callsign is null or trim(callsign) = '' then 'Unknown'
            else callsign
        end as callsign,
        case
            when country is null or trim(country) = '' then 'Unknown'
            else country
        end as country,
        case
            when is_active in ('Y', 'Yes', '1', 1, true,'YES') then true
            else false
        end as is_active
    from src
),

filtered as (
    select *
    from cleaned
    where airline_id is not null
      and airline_name is not null
      and airline_name != '' 
      and lower(airline_name) not like 'unknown%'
),

dedup as (  
    select
        *,
        row_number() over (partition by airline_id order by airline_name) as rn
    from filtered
)

select
    airline_id,
    {{ dbt_utils.generate_surrogate_key(['airline_id']) }} as airline_sk,
    airline_name,
    alias,
    iata,
    icao,
    callsign,
    country,
    is_active
from dedup
where rn = 1
