-- Mart model: anomalous flight events from telemetry
WITH staged AS (
    SELECT * FROM {{ ref('stg_aircraft_telemetry') }}
)

SELECT
    icao24,
    callsign,
    origin_country,
    contact_timestamp,
    latitude,
    longitude,
    altitude_meters,
    velocity_kmh,
    vertical_rate_mps,
    squawk,
    CASE
        WHEN squawk IN ('7500', '7600', '7700') THEN 'EMERGENCY_SQUAWK'
        WHEN abs(vertical_rate_mps) >= 15.0 THEN 'EXTREME_VERTICAL_RATE'
        WHEN velocity_kmh > 1050.0 THEN 'SUPERSONIC_OR_SPIKE'
        ELSE 'NORMAL'
    END AS anomaly_classification
FROM staged
WHERE squawk IN ('7500', '7600', '7700')
   OR abs(vertical_rate_mps) >= 15.0
   OR velocity_kmh > 1050.0
