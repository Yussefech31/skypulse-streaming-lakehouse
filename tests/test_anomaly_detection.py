"""Unit tests for flight anomaly detection."""

from src.ingestion.schema import AircraftTelemetry
from src.streaming.anomaly_detector import AnomalyDetector


def test_emergency_squawk_detection():
    detector = AnomalyDetector()
    record = AircraftTelemetry(
        icao24="abc777",
        callsign="EMERG1",
        origin_country="Morocco",
        last_contact=1700000000,
        squawk="7700",  # General Emergency
    )
    anomalies = detector.process_record(record)
    assert len(anomalies) == 1
    assert anomalies[0].anomaly_type == "EMERGENCY_SQUAWK"
    assert anomalies[0].severity == "CRITICAL"


def test_extreme_vertical_rate_detection():
    detector = AnomalyDetector(vertical_rate_threshold=15.0)
    record = AircraftTelemetry(
        icao24="dive01",
        callsign="DIVE99",
        origin_country="Morocco",
        last_contact=1700000000,
        vertical_rate=-18.5,  # -18.5 m/s steep dive
    )
    anomalies = detector.process_record(record)
    assert len(anomalies) == 1
    assert anomalies[0].anomaly_type == "EXTREME_VERTICAL_RATE"
    assert "dive" in anomalies[0].description


def test_sudden_velocity_jump_detection():
    detector = AnomalyDetector(speed_delta_threshold=25.0)
    r1 = AircraftTelemetry(
        icao24="speed01",
        callsign="FAST01",
        origin_country="Morocco",
        last_contact=1700000000,
        velocity=200.0,
    )
    r2 = AircraftTelemetry(
        icao24="speed01",
        callsign="FAST01",
        origin_country="Morocco",
        last_contact=1700000010,  # 10s later
        velocity=235.0,  # +35 m/s jump
    )
    detector.process_record(r1)
    anomalies = detector.process_record(r2)
    assert len(anomalies) == 1
    assert anomalies[0].anomaly_type == "SUDDEN_VELOCITY_CHANGE"
