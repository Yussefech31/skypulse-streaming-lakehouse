"""Lakehouse storage engine supporting MinIO S3 and local partitioned Parquet tables."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config.config import settings
from src.ingestion.schema import AircraftTelemetry
from src.streaming.anomaly_detector import AnomalyEvent
from src.streaming.window_aggregations import WindowMetrics

logger = logging.getLogger(__name__)


class LakehouseStorage:
    """Manages reading and writing partitioned Parquet tables for the Lakehouse."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = base_dir or settings.local_lakehouse_dir
        self.silver_dir = self.base_dir / "silver" / "aircraft_telemetry"
        self.gold_agg_dir = self.base_dir / "gold" / "windowed_aggregations"
        self.gold_anomalies_dir = self.base_dir / "gold" / "anomalies"

        # Ensure directory structures exist
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.gold_agg_dir.mkdir(parents=True, exist_ok=True)
        self.gold_anomalies_dir.mkdir(parents=True, exist_ok=True)

    def write_silver_batch(
        self, records: list[AircraftTelemetry], batch_id: str | None = None
    ) -> Path | None:
        """Write a streaming micro-batch of telemetry into silver layer partitioned by hour."""
        if not records:
            return None

        now = datetime.now(UTC)
        part_dir = (
            self.silver_dir
            / f"year={now.year}"
            / f"month={now.month:02d}"
            / f"day={now.day:02d}"
            / f"hour={now.hour:02d}"
        )
        part_dir.mkdir(parents=True, exist_ok=True)

        batch_name = batch_id or f"batch_{int(now.timestamp() * 1000)}"
        file_path = part_dir / f"telemetry_{batch_name}.parquet"

        data = [r.model_dump() for r in records]
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression="snappy")

        logger.debug(
            "Wrote silver batch %s with %d records to %s", batch_name, len(records), file_path
        )
        return file_path

    def write_gold_aggregations(
        self, metrics: list[WindowMetrics], batch_id: str | None = None
    ) -> Path | None:
        """Write window aggregations into gold layer."""
        if not metrics:
            return None

        now = datetime.now(UTC)
        part_dir = self.gold_agg_dir / f"year={now.year}" / f"month={now.month:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)

        batch_name = batch_id or f"agg_{int(now.timestamp() * 1000)}"
        file_path = part_dir / f"window_metrics_{batch_name}.parquet"

        df = pd.DataFrame([m.to_dict() for m in metrics])
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression="snappy")

        return file_path

    def write_gold_anomalies(
        self, anomalies: list[AnomalyEvent], batch_id: str | None = None
    ) -> Path | None:
        """Write detected flight anomalies into gold layer."""
        if not anomalies:
            return None

        now = datetime.now(UTC)
        part_dir = self.gold_anomalies_dir / f"year={now.year}" / f"month={now.month:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)

        batch_name = batch_id or f"anom_{int(now.timestamp() * 1000)}"
        file_path = part_dir / f"anomalies_{batch_name}.parquet"

        df = pd.DataFrame([a.to_dict() for a in anomalies])
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression="snappy")

        return file_path

    def read_silver_records(self) -> pd.DataFrame:
        """Read all silver telemetry parquet files as a unified pandas DataFrame."""
        files = list(self.silver_dir.glob("**/*.parquet"))
        if not files:
            return pd.DataFrame()
        dfs = [pq.ParquetFile(f).read().to_pandas() for f in files]
        return pd.concat(dfs, ignore_index=True)

    def read_gold_aggregations(self) -> pd.DataFrame:
        """Read all gold window aggregations."""
        files = list(self.gold_agg_dir.glob("**/*.parquet"))
        if not files:
            return pd.DataFrame()
        dfs = [pq.ParquetFile(f).read().to_pandas() for f in files]
        return pd.concat(dfs, ignore_index=True)

    def read_gold_anomalies(self) -> pd.DataFrame:
        """Read all gold anomalies."""
        files = list(self.gold_anomalies_dir.glob("**/*.parquet"))
        if not files:
            return pd.DataFrame()
        dfs = [pq.ParquetFile(f).read().to_pandas() for f in files]
        return pd.concat(dfs, ignore_index=True)
