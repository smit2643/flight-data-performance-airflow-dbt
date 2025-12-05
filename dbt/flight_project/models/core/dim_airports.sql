{{ config(
    schema='core',
    materialized='table'
) }}

with src as (
    select *
    from {{ ref('stg_airports') }}
),

clean as (
    select
        airport_id,
        name as airport_name,
        city,
        country,
        iata as iata_code,
        icao as icao_code,
        latitude,
        longitude,
        altitude as altitude_ft,
        tz_name as timezone_name,
        row_number() over (partition by airport_id order by airport_name) as rn
    from src
),

airport_name_seed as (
    select
        trim(name) as seed_name,
        trim(cleaned_name) as seed_cleaned_name
    from {{ ref('lowercase_airport_names') }}
),

city_seed as (
    select
        trim(city) as seed_city,
        trim(cleaned_city) as seed_cleaned_city
    from {{ ref('lowercase_city_names') }}
),

final_prep as (

    select
        c.airport_id,
        {{ dbt_utils.generate_surrogate_key(['airport_id']) }} as airport_sk,
        coalesce(
            trim(a.seed_cleaned_name),
            case
                when c.airport_name is not null
                    and substr(trim(c.airport_name),1,1) between 'a' and 'z'
                then 'N' || trim(c.airport_name)
                when c.airport_name is not null then initcap(trim(c.airport_name))
                else null
            end
        ) as airport_name_cleaned,

        coalesce(
            trim(s.seed_cleaned_city),
            case
                when c.city is not null
                    and substr(trim(c.city),1,1) between 'a' and 'z'
                then 'N' || trim(c.city)
                when c.city is not null then initcap(trim(c.city))
                else null
            end
        ) as city_cleaned,

        c.country,
        c.iata_code,
        c.icao_code,
        c.latitude,
        c.longitude,
        c.altitude_ft,
        c.timezone_name,
        c.rn

    from clean c

    left join airport_name_seed a
      on lower(trim(c.airport_name)) = lower(trim(a.seed_name))

    left join city_seed s
      on lower(trim(c.city)) = lower(trim(s.seed_city))
)

select
    airport_id,
    airport_sk,
    airport_name_cleaned as airport_name,
    city_cleaned as city,
    country,
    iata_code,
    icao_code,
    latitude,
    longitude,
    altitude_ft,
    timezone_name

from final_prep
where rn = 1
  and iata_code is not null
