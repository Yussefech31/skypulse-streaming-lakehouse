"""Unit tests for Lakehouse storage and compaction."""

from pathlib import Path

from src.ingestion.schema import AircraftTelemetry
from src.lakehouse.compactor import LakehouseCompactor
from src.lakehouse.storage import LakehouseStorage


def test_storage_write_and_read(tmp_path: Path):
    storage = LakehouseStorage(base_dir=tmp_path)
    records = [
        AircraftTelemetry(
            icao24="test01",
            callsign="TST01",
            origin_country="Morocco",
            last_contact=1700000000,
            baro_altitude=10000.0,
            velocity=200.0,
        ),
        AircraftTelemetry(
            icao24="test02",
            callsign="TST02",
            origin_country="Spain",
            last_contact=1700000005,
            baro_altitude=11000.0,
            velocity=220.0,
        ),
    ]

    p = storage.write_silver_batch(records, batch_id="b1")
    assert p is not None
    assert p.exists()

    df = storage.read_silver_records()
    assert len(df) == 2
    assert "icao24" in df.columns


def test_compactor_merges_multiple_batches(tmp_path: Path):
    storage = LakehouseStorage(base_dir=tmp_path)
    rec1 = [AircraftTelemetry(icao24="a1", origin_country="Morocco", last_contact=1700000000)]
    rec2 = [AircraftTelemetry(icao24="a2", origin_country="Morocco", last_contact=1700000010)]

    storage.write_silver_batch(rec1, batch_id="batch_01")
    storage.write_silver_batch(rec2, batch_id="batch_02")

    compactor = LakehouseCompactor(base_dir=tmp_path)
    stats = compactor.run_compaction()

    assert stats.files_merged == 2
    assert stats.records_compacted == 2
    assert stats.partitions_processed == 1

    df = storage.read_silver_records()
    assert len(df) == 2
