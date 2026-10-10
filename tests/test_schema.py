"""Unit tests for telemetry schema validation."""

from src.ingestion.schema import AircraftTelemetry


def test_valid_opensky_vector_parsing():
    raw_vector = [
        "400a0b",
        "BAW123  ",
        "United Kingdom",
        1700000000,
        1700000005,
        -0.1278,
        51.5074,
        10000.0,
        False,
        220.5,
        180.0,
        -2.5,
        None,
        10200.0,
        "1000",
        False,
        0,
    ]
    telemetry = AircraftTelemetry.from_opensky_vector(raw_vector)
    assert telemetry is not None
    assert telemetry.icao24 == "400a0b"
    assert telemetry.callsign == "BAW123"
    assert telemetry.origin_country == "United Kingdom"
    assert telemetry.latitude == 51.5074
    assert telemetry.longitude == -0.1278
    assert telemetry.velocity == 220.5
    assert telemetry.velocity_kmh == round(220.5 * 3.6, 2)
    assert telemetry.on_ground is False


def test_invalid_vector_length_returns_none():
    short_vector = ["400a0b", "BAW123"]
    assert AircraftTelemetry.from_opensky_vector(short_vector) is None


def test_invalid_coordinate_bounds():
    raw_vector = [
        "400a0b",
        "BAW123",
        "United Kingdom",
        1700000000,
        1700000005,
        250.0,  # invalid longitude (> 180)
        95.0,  # invalid latitude (> 90)
        1000.0,
        False,
        200.0,
        0.0,
        0.0,
        None,
        1000.0,
        "1000",
        False,
        0,
    ]
    assert AircraftTelemetry.from_opensky_vector(raw_vector) is None


def test_empty_icao24_validation_error():
    raw_vector = [
        "   ",
        "BAW123",
        "United Kingdom",
        1700000000,
        1700000005,
        0.0,
        0.0,
        1000.0,
        False,
        200.0,
        0.0,
        0.0,
        None,
        1000.0,
        "1000",
        False,
        0,
    ]
    assert AircraftTelemetry.from_opensky_vector(raw_vector) is None
