"""Unit tests for DuckDB SQL data quality assertions."""

from pathlib import Path

from src.ingestion.schema import AircraftTelemetry
from src.lakehouse.storage import LakehouseStorage
from src.quality.assertions import DataQualityChecker


def test_data_quality_clean_data(tmp_path: Path):
    storage = LakehouseStorage(base_dir=tmp_path)
    clean_records = [
        AircraftTelemetry(
            icao24="clean1",
            origin_country="Morocco",
            last_contact=1700000000,
            latitude=33.5,
            longitude=-7.5,
            velocity=210.0,
            baro_altitude=9000.0,
        ),
        AircraftTelemetry(
            icao24="clean2",
            origin_country="France",
            last_contact=1700000005,
            latitude=48.8,
            longitude=2.3,
            velocity=230.0,
            baro_altitude=10500.0,
        ),
    ]
    storage.write_silver_batch(clean_records, batch_id="clean_batch")

    checker = DataQualityChecker(lakehouse_dir=tmp_path)
    report = checker.run_checks()

    assert report.overall_passed is True
    assert report.total_records_evaluated == 2
    assert report.failed_rules == 0
    assert report.total_rules == 5


def test_data_quality_detects_violations(tmp_path: Path):
    storage = LakehouseStorage(base_dir=tmp_path)
    # Directly write record with boundary anomaly to test SQL detection
    corrupted_records = [
        AircraftTelemetry(
            icao24="bad_vel",
            origin_country="Test",
            last_contact=1700000000,
            velocity=1500.0,  # exceeds 1000 m/s rule
        )
    ]
    storage.write_silver_batch(corrupted_records, batch_id="bad_batch")

    checker = DataQualityChecker(lakehouse_dir=tmp_path)
    report = checker.run_checks()

    assert report.overall_passed is False
    assert report.failed_rules >= 1
    failed_rule = next(r for r in report.results if not r.passed)
    assert failed_rule.rule_name == "velocity_sanity"
