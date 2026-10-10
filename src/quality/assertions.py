"""Data Quality suite powered by DuckDB SQL assertions on Lakehouse Parquet tables."""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from config.config import settings

logger = logging.getLogger(__name__)


@dataclass
class QualityResult:
    """Result of an individual data quality rule check."""

    rule_name: str
    description: str
    passed: bool
    violations_count: int
    total_records: int
    query_executed: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DataQualityReport:
    """Consolidated report across all executed quality assertions."""

    timestamp: str
    overall_passed: bool
    total_rules: int
    passed_rules: int
    failed_rules: int
    total_records_evaluated: int
    execution_time_seconds: float
    results: list[QualityResult]

    def to_dict(self) -> dict:
        data = asdict(self)
        data["results"] = [r.to_dict() for r in self.results]
        return data


class DataQualityChecker:
    """Executes high-performance in-memory SQL assertions against Lakehouse tables."""

    def __init__(self, lakehouse_dir: Path | None = None) -> None:
        self.lakehouse_dir = lakehouse_dir or settings.local_lakehouse_dir
        self.silver_pattern = str(
            (self.lakehouse_dir / "silver" / "aircraft_telemetry" / "**/*.parquet").as_posix()
        )

    def run_checks(self) -> DataQualityReport:
        """Run all data quality assertion rules against silver telemetry parquet files."""
        start_time = time.perf_counter()
        con = duckdb.connect(database=":memory:")

        # Check if files exist
        has_files = (
            len(list(Path(self.lakehouse_dir).glob("silver/aircraft_telemetry/**/*.parquet"))) > 0
        )
        if not has_files:
            elapsed = time.perf_counter() - start_time
            return DataQualityReport(
                timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                overall_passed=True,
                total_rules=0,
                passed_rules=0,
                failed_rules=0,
                total_records_evaluated=0,
                execution_time_seconds=round(elapsed, 4),
                results=[],
            )

        # Create temporary view over parquet files
        con.execute(
            f"CREATE VIEW silver_telemetry AS SELECT * FROM read_parquet('{self.silver_pattern}')"
        )
        total_rows = con.execute("SELECT count(*) FROM silver_telemetry").fetchone()[0]

        rules = [
            (
                "no_null_icao24",
                "Unique ICAO24 transponder identifier must never be null or empty",
                "SELECT count(*) FROM silver_telemetry WHERE icao24 IS NULL OR trim(icao24) = ''",
            ),
            (
                "coordinate_bounds",
                "Latitude must be in [-90, 90] and Longitude in [-180, 180]",
                """
                SELECT count(*) FROM silver_telemetry
                WHERE (latitude IS NOT NULL AND (latitude < -90.0 OR latitude > 90.0))
                   OR (longitude IS NOT NULL AND (longitude < -180.0 OR longitude > 180.0))
                """,
            ),
            (
                "velocity_sanity",
                "Velocity over ground must be non-negative and realistic (<= 1000 m/s / Mach 3)",
                """
                SELECT count(*) FROM silver_telemetry
                WHERE velocity IS NOT NULL AND (velocity < 0.0 OR velocity > 1000.0)
                """,
            ),
            (
                "altitude_sanity",
                "Barometric altitude must be within physical flight boundaries [-500m, 25000m]",
                """
                SELECT count(*) FROM silver_telemetry
                WHERE baro_altitude IS NOT NULL AND (baro_altitude < -500.0 OR baro_altitude > 25000.0)
                """,
            ),
            (
                "valid_contact_time",
                "Last contact timestamp must be positive and non-zero",
                "SELECT count(*) FROM silver_telemetry WHERE last_contact <= 0",
            ),
        ]

        results: list[QualityResult] = []
        for name, desc, query in rules:
            violations = con.execute(query).fetchone()[0]
            passed = violations == 0
            results.append(
                QualityResult(
                    rule_name=name,
                    description=desc,
                    passed=passed,
                    violations_count=violations,
                    total_records=total_rows,
                    query_executed=query.strip(),
                )
            )

        con.close()
        elapsed = time.perf_counter() - start_time
        failed_count = sum(1 for r in results if not r.passed)

        return DataQualityReport(
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            overall_passed=failed_count == 0,
            total_rules=len(rules),
            passed_rules=len(rules) - failed_count,
            failed_rules=failed_count,
            total_records_evaluated=total_rows,
            execution_time_seconds=round(elapsed, 4),
            results=results,
        )
