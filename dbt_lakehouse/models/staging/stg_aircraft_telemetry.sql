-- Staging model: cleaned and typed telemetry records
WITH raw_telemetry AS (
    SELECT * FROM read_parquet('lakehouse/silver/aircraft_telemetry/**/*.parquet')
)

SELECT
    icao24,
    trim(callsign) AS callsign,
    COALESCE(origin_country, 'Unknown') AS origin_country,
    to_timestamp(last_contact) AS contact_timestamp,
    round(cast(latitude AS DOUBLE), 5) AS latitude,
    round(cast(longitude AS DOUBLE), 5) AS longitude,
    cast(baro_altitude AS DOUBLE) AS altitude_meters,
    round(cast(baro_altitude * 3.28084 AS DOUBLE), 1) AS altitude_feet,
    cast(velocity AS DOUBLE) AS velocity_mps,
    round(cast(velocity * 3.6 AS DOUBLE), 1) AS velocity_kmh,
    cast(vertical_rate AS DOUBLE) AS vertical_rate_mps,
    on_ground,
    squawk,
    ingested_at
FROM raw_telemetry
WHERE icao24 IS NOT NULL
