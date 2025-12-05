{{ config(materialized='view') }}

WITH src AS (
    SELECT
        airport_id,
        name,
        city,
        country,
        iata,
        icao,
        latitude,
        longitude,
        altitude,
        utc_offset,
        dst,
        tz_name,
        type,
        source
    FROM {{ source('raw','AIRPORTS_RAW') }}
),
clean AS (
    SELECT
        TRY_TO_NUMBER(airport_id) AS airport_id,
        NULLIF(REPLACE(name, '\N', ''), '') AS name,
        NULLIF(REPLACE(city, '\N', ''), '') AS city,
        NULLIF(REPLACE(country, '\N', ''), '') AS country,
        NULLIF(REPLACE(iata, '\N', ''), '') AS iata,
        NULLIF(REPLACE(icao, '\N', ''), '') AS icao,
        TRY_TO_DOUBLE(NULLIF(REPLACE(latitude, '\N', ''), '')) AS latitude,
        TRY_TO_DOUBLE(NULLIF(REPLACE(longitude, '\N', ''), '')) AS longitude,
        TRY_TO_NUMBER(NULLIF(REPLACE(altitude, '\N', ''), '')) AS altitude,
        TRY_TO_NUMBER(NULLIF(REPLACE(utc_offset, '\N', ''), '')) AS utc_offset,
        UPPER(NULLIF(REPLACE(dst, '\N', ''), '')) AS dst,
        NULLIF(REPLACE(tz_name, '\N', ''), '') AS tz_name,
        NULLIF(REPLACE(type, '\N', ''), '') AS airport_type,
        NULLIF(REPLACE(source, '\N', ''), '') AS source_system
    FROM src)
SELECT *
FROM clean
WHERE airport_id IS NOT NULL

