"""Unit tests for windowed aggregations."""

from src.ingestion.schema import AircraftTelemetry
from src.streaming.window_aggregations import WindowAggregator


def test_window_aggregation_calculations():
    records = [
        AircraftTelemetry(
            icao24="abc001",
            callsign="RAM101",
            origin_country="Morocco",
            last_contact=1700000010,
            baro_altitude=10000.0,
            velocity=220.0,
            vertical_rate=2.0,  # Climbing
        ),
        AircraftTelemetry(
            icao24="abc002",
            callsign="RAM102",
            origin_country="Morocco",
            last_contact=1700000020,
            baro_altitude=8000.0,
            velocity=200.0,
            vertical_rate=-3.0,  # Descending
        ),
        AircraftTelemetry(
            icao24="abc003",
            callsign="AFR201",
            origin_country="France",
            last_contact=1700000030,
            baro_altitude=11000.0,
            velocity=240.0,
            vertical_rate=0.0,  # Level
        ),
    ]

    aggregator = WindowAggregator(window_minutes=5)
    metrics = aggregator.compute_windowed_aggregations(records)

    assert len(metrics) == 2  # Morocco and France
    morocco_metric = next(m for m in metrics if m.region == "Morocco")
    assert morocco_metric.unique_aircraft_count == 2
    assert morocco_metric.total_observations == 2
    assert morocco_metric.avg_altitude_m == 9000.0
    assert morocco_metric.avg_velocity_mps == 210.0
    assert morocco_metric.max_velocity_mps == 220.0
    assert morocco_metric.climbing_count == 1
    assert morocco_metric.descending_count == 1
    assert morocco_metric.level_flight_count == 0


def test_empty_records_returns_empty_metrics():
    aggregator = WindowAggregator(window_minutes=5)
    assert aggregator.compute_windowed_aggregations([]) == []
