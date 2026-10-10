"""Unit tests for OpenSky ingestion client."""

from src.ingestion.opensky_client import OpenSkyClient


def test_load_local_snapshot():
    client = OpenSkyClient()
    snapshot = client.load_local_snapshot()
    assert "states" in snapshot
    assert isinstance(snapshot["states"], list)
    assert len(snapshot["states"]) > 0


def test_fetch_telemetry_batch_parses_records():
    client = OpenSkyClient()
    batch = client.fetch_telemetry_batch()
    assert len(batch) > 0
    first = batch[0]
    assert first.icao24
    assert first.origin_country


def test_bounding_box_params_construction():
    client = OpenSkyClient(bbox=(27.5, -14.0, 36.5, -1.0))
    params = client._build_params()
    assert params["lamin"] == 27.5
    assert params["lomin"] == -14.0
    assert params["lamax"] == 36.5
    assert params["lomax"] == -1.0
