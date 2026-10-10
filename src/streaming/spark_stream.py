"""Streaming telemetry engine with windowed aggregations and lakehouse sinks."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from config.config import settings
from src.ingestion.opensky_client import OpenSkyClient
from src.ingestion.schema import AircraftTelemetry
from src.lakehouse.storage import LakehouseStorage
from src.streaming.anomaly_detector import AnomalyDetector
from src.streaming.window_aggregations import WindowAggregator

logger = logging.getLogger(__name__)


class StreamingTelemetryEngine:
    """Micro-batch streaming execution engine with windowing and anomaly sinks."""

    def __init__(
        self,
        storage: LakehouseStorage | None = None,
        window_minutes: int | None = None,
    ) -> None:
        self.storage = storage or LakehouseStorage()
        self.aggregator = WindowAggregator(
            window_minutes=window_minutes or settings.window_duration_minutes
        )
        self.anomaly_detector = AnomalyDetector()
        self.metrics: dict[str, Any] = {
            "batches_processed": 0,
            "records_processed": 0,
            "anomalies_detected": 0,
            "total_processing_time_ms": 0.0,
        }

    def process_micro_batch(
        self, records: list[AircraftTelemetry], batch_id: str | None = None
    ) -> dict[str, Any]:
        """Execute complete streaming cycle for a micro-batch:

        1. Write raw validated records to Silver Lakehouse
        2. Detect in-flight telemetry anomalies & persist to Gold
        3. Compute windowed aggregations & persist to Gold
        """
        start = time.perf_counter()
        bid = batch_id or f"mb_{int(time.time() * 1000)}"

        # 1. Silver Sink
        silver_path = self.storage.write_silver_batch(records, batch_id=bid)

        # 2. Anomaly Detection & Gold Sink
        anomalies = self.anomaly_detector.process_batch(records)
        anomalies_path = self.storage.write_gold_anomalies(anomalies, batch_id=bid)

        # 3. Windowed Aggregations & Gold Sink
        window_metrics = self.aggregator.compute_windowed_aggregations(records)
        agg_path = self.storage.write_gold_aggregations(window_metrics, batch_id=bid)

        elapsed_ms = (time.perf_counter() - start) * 1000.0

        self.metrics["batches_processed"] += 1
        self.metrics["records_processed"] += len(records)
        self.metrics["anomalies_detected"] += len(anomalies)
        self.metrics["total_processing_time_ms"] += elapsed_ms

        summary = {
            "batch_id": bid,
            "records_count": len(records),
            "anomalies_count": len(anomalies),
            "window_aggregations_count": len(window_metrics),
            "processing_latency_ms": round(elapsed_ms, 2),
            "silver_path": str(silver_path) if silver_path else None,
            "anomalies_path": str(anomalies_path) if anomalies_path else None,
            "aggregations_path": str(agg_path) if agg_path else None,
        }
        return summary


def run_standalone_stream(batches: int = 3, batch_size: int = 2000) -> None:
    """Run standalone stream cycle using real OpenSky snapshot data."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    client = OpenSkyClient()
    engine = StreamingTelemetryEngine()

    all_records = client.fetch_telemetry_batch()
    logger.info("Total available records in snapshot: %d", len(all_records))

    for i in range(batches):
        start_idx = (i * batch_size) % len(all_records)
        batch = all_records[start_idx : start_idx + batch_size]
        res = engine.process_micro_batch(batch, batch_id=f"stream_batch_{i + 1}")
        logger.info("Processed batch %d: %s", i + 1, json.dumps(res, indent=2))
        time.sleep(1)


if __name__ == "__main__":
    run_standalone_stream()
