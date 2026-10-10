-- Mart model: 5-minute windowed flight aggregations per origin country
WITH staged AS (
    SELECT * FROM {{ ref('stg_aircraft_telemetry') }}
)

SELECT
    time_bucket(INTERVAL '5 minutes', contact_timestamp) AS window_start,
    time_bucket(INTERVAL '5 minutes', contact_timestamp) + INTERVAL '5 minutes' AS window_end,
    origin_country,
    count(DISTINCT icao24) AS unique_aircraft_count,
    count(*) AS total_pings,
    round(avg(altitude_meters), 1) AS avg_altitude_meters,
    round(avg(velocity_kmh), 1) AS avg_velocity_kmh,
    round(max(velocity_kmh), 1) AS max_velocity_kmh,
    count(CASE WHEN vertical_rate_mps > 1.5 THEN 1 END) AS climbing_count,
    count(CASE WHEN vertical_rate_mps < -1.5 THEN 1 END) AS descending_count,
    count(CASE WHEN abs(vertical_rate_mps) <= 1.5 THEN 1 END) AS level_flight_count
FROM staged
GROUP BY 1, 2, 3
ORDER BY total_pings DESC
