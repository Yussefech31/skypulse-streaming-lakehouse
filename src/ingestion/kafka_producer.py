"""Kafka Producer for publishing validated ADS-B aircraft telemetry."""

from __future__ import annotations

import json
import logging
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.config import settings
from src.ingestion.opensky_client import OpenSkyClient
from src.ingestion.schema import AircraftTelemetry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


class TelemetryKafkaProducer:
    """Producer responsible for streaming parsed ADS-B messages into Kafka."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        mock_mode: bool = False,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_topic_telemetry
        self.mock_mode = mock_mode
        self._producer: KafkaProducer | None = None
        self.metrics: dict[str, int | float] = {
            "messages_sent": 0,
            "bytes_sent": 0,
            "errors": 0,
        }

        if not self.mock_mode:
            self._init_producer()

    def _init_producer(self) -> None:
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else b"",
                acks=1,
                retries=3,
                linger_ms=10,
                max_in_flight_requests_per_connection=5,
            )
            logger.info("Connected to Kafka at %s", self.bootstrap_servers)
        except KafkaError as e:
            logger.warning("Kafka initialization failed (%s). Operating in mock mode.", e)
            self.mock_mode = True

    def publish_record(self, record: AircraftTelemetry) -> bool:
        """Publish a single telemetry record with partition key icao24."""
        payload = record.model_dump()
        payload_bytes = json.dumps(payload).encode("utf-8")

        if self.mock_mode or self._producer is None:
            self.metrics["messages_sent"] += 1
            self.metrics["bytes_sent"] += len(payload_bytes)
            return True

        try:
            future = self._producer.send(
                self.topic,
                key=record.icao24,
                value=payload,
            )
            # Non-blocking async send with metadata callback
            future.add_errback(self._on_send_error)
            self.metrics["messages_sent"] += 1
            self.metrics["bytes_sent"] += len(payload_bytes)
            return True
        except Exception as e:
            logger.error("Failed to enqueue message for icao24 %s: %s", record.icao24, e)
            self.metrics["errors"] += 1
            return False

    def publish_batch(self, records: list[AircraftTelemetry]) -> int:
        """Publish a list of telemetry records. Returns count of successful records."""
        success = 0
        for record in records:
            if self.publish_record(record):
                success += 1
        self.flush()
        return success

    def _on_send_error(self, exc: Exception) -> None:
        logger.error("Kafka send error: %s", exc)
        self.metrics["errors"] += 1

    def flush(self) -> None:
        if self._producer:
            self._producer.flush()

    def close(self) -> None:
        if self._producer:
            self._producer.close()


def run_ingestion_loop(iterations: int = 1, interval_sec: int = 10) -> None:
    """Continuously poll OpenSky Network and push records to Kafka."""
    client = OpenSkyClient()
    producer = TelemetryKafkaProducer()

    logger.info("Starting ingestion loop (iterations=%d, interval=%ds)", iterations, interval_sec)
    for i in range(iterations):
        start = time.perf_counter()
        records = client.fetch_telemetry_batch()
        sent = producer.publish_batch(records)
        elapsed = time.perf_counter() - start
        logger.info(
            "Iteration %d/%d: Processed %d records (%d sent) in %.2fs (%.1f msg/sec)",
            i + 1,
            iterations,
            len(records),
            sent,
            elapsed,
            len(records) / elapsed if elapsed > 0 else 0,
        )
        if i + 1 < iterations:
            time.sleep(interval_sec)

    producer.close()
    logger.info("Ingestion completed. Final metrics: %s", producer.metrics)


if __name__ == "__main__":
    run_ingestion_loop(iterations=2, interval_sec=5)
