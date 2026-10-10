"""Real-time anomaly detection for streaming aircraft telemetry."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from config.config import settings
from src.ingestion.schema import AircraftTelemetry


@dataclass
class AnomalyEvent:
    """Structured event emitted when anomalous flight telemetry is detected."""

    icao24: str
    callsign: str | None
    origin_country: str
    anomaly_type: str
    severity: str  # INFO, WARNING, CRITICAL
    description: str
    latitude: float | None
    longitude: float | None
    baro_altitude: float | None
    velocity: float | None
    vertical_rate: float | None
    timestamp: str

    def to_dict(self) -> dict:
        return asdict(self)


class AnomalyDetector:
    """Detects flight telemetry anomalies across consecutive state updates."""

    EMERGENCY_SQUAWKS = {
        "7500": ("Unlawful Interference (Hijack)", "CRITICAL"),
        "7600": ("Radio Communication Failure", "WARNING"),
        "7700": ("General In-flight Emergency", "CRITICAL"),
    }

    def __init__(
        self,
        speed_delta_threshold: float | None = None,
        vertical_rate_threshold: float | None = None,
    ) -> None:
        self.speed_delta_threshold = (
            speed_delta_threshold or settings.speed_change_anomaly_threshold_mps
        )
        self.vertical_rate_threshold = (
            vertical_rate_threshold or settings.vertical_rate_anomaly_threshold_mps
        )
        # In-memory aircraft state cache: icao24 -> (last_velocity, last_timestamp)
        self._aircraft_history: dict[str, tuple[float, int]] = {}

    def process_record(self, record: AircraftTelemetry) -> list[AnomalyEvent]:
        """Evaluate a single telemetry message for anomalies."""
        anomalies: list[AnomalyEvent] = []
        now_iso = datetime.now(UTC).isoformat()

        # 1. Emergency Squawk Check
        if record.squawk and record.squawk in self.EMERGENCY_SQUAWKS:
            desc, severity = self.EMERGENCY_SQUAWKS[record.squawk]
            anomalies.append(
                AnomalyEvent(
                    icao24=record.icao24,
                    callsign=record.callsign,
                    origin_country=record.origin_country,
                    anomaly_type="EMERGENCY_SQUAWK",
                    severity=severity,
                    description=f"Transponder set to {record.squawk}: {desc}",
                    latitude=record.latitude,
                    longitude=record.longitude,
                    baro_altitude=record.baro_altitude,
                    velocity=record.velocity,
                    vertical_rate=record.vertical_rate,
                    timestamp=now_iso,
                )
            )

        # 2. Extreme Vertical Rate Check (Steep Climb / Rapid Descent)
        if (
            record.vertical_rate is not None
            and abs(record.vertical_rate) >= self.vertical_rate_threshold
        ):
            action = "dive" if record.vertical_rate < 0 else "climb"
            rate_fpm = round(record.vertical_rate * 196.85, 0)
            anomalies.append(
                AnomalyEvent(
                    icao24=record.icao24,
                    callsign=record.callsign,
                    origin_country=record.origin_country,
                    anomaly_type="EXTREME_VERTICAL_RATE",
                    severity="WARNING",
                    description=(
                        f"Abnormal {action} rate of {record.vertical_rate:.1f} m/s "
                        f"(~{rate_fpm} ft/min)"
                    ),
                    latitude=record.latitude,
                    longitude=record.longitude,
                    baro_altitude=record.baro_altitude,
                    velocity=record.velocity,
                    vertical_rate=record.vertical_rate,
                    timestamp=now_iso,
                )
            )

        # 3. Sudden Velocity Change Check (Acceleration / Deceleration Spike)
        if record.velocity is not None:
            ts = record.time_position or record.last_contact
            if record.icao24 in self._aircraft_history:
                prev_vel, prev_ts = self._aircraft_history[record.icao24]
                dt = max(1, ts - prev_ts)
                delta_vel = abs(record.velocity - prev_vel)

                # Flag if delta velocity exceeds threshold over short interval (<= 30s)
                if dt <= 30 and delta_vel >= self.speed_delta_threshold:
                    anomalies.append(
                        AnomalyEvent(
                            icao24=record.icao24,
                            callsign=record.callsign,
                            origin_country=record.origin_country,
                            anomaly_type="SUDDEN_VELOCITY_CHANGE",
                            severity="WARNING",
                            description=(
                                f"Velocity changed by {delta_vel:.1f} m/s in {dt}s "
                                f"({prev_vel:.1f} -> {record.velocity:.1f} m/s)"
                            ),
                            latitude=record.latitude,
                            longitude=record.longitude,
                            baro_altitude=record.baro_altitude,
                            velocity=record.velocity,
                            vertical_rate=record.vertical_rate,
                            timestamp=now_iso,
                        )
                    )

            self._aircraft_history[record.icao24] = (record.velocity, ts)

        return anomalies

    def process_batch(self, records: list[AircraftTelemetry]) -> list[AnomalyEvent]:
        """Process batch of telemetry and return all detected anomalies."""
        all_anomalies: list[AnomalyEvent] = []
        for r in records:
            all_anomalies.extend(self.process_record(r))
        return all_anomalies
