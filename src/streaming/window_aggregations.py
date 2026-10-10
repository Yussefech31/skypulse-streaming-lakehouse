"""Windowed aggregation logic for streaming aircraft telemetry."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import pandas as pd

from src.ingestion.schema import AircraftTelemetry


@dataclass
class WindowMetrics:
    """Metrics aggregated per region and time window."""

    window_start: str
    window_end: str
    region: str
    unique_aircraft_count: int
    total_observations: int
    avg_altitude_m: float
    avg_velocity_mps: float
    max_velocity_mps: float
    climbing_count: int
    descending_count: int
    level_flight_count: int

    def to_dict(self) -> dict:
        return asdict(self)


class WindowAggregator:
    """Computes tumbling and sliding window aggregations over streams."""

    def __init__(self, window_minutes: int = 5) -> None:
        self.window_seconds = window_minutes * 60

    def compute_windowed_aggregations(
        self, records: list[AircraftTelemetry]
    ) -> list[WindowMetrics]:
        """Aggregate telemetry records grouped by 5-minute window and origin country/region."""
        if not records:
            return []

        rows = []
        for r in records:
            # Anchor timestamp to last_contact or time_position
            ts = r.time_position or r.last_contact
            rows.append(
                {
                    "icao24": r.icao24,
                    "region": r.origin_country or "Unknown",
                    "timestamp": ts,
                    "altitude": r.baro_altitude,
                    "velocity": r.velocity,
                    "vertical_rate": r.vertical_rate,
                }
            )

        df = pd.DataFrame(rows)

        # Bucket timestamp into 5-minute tumbling windows
        df["window_bucket"] = (df["timestamp"] // self.window_seconds) * self.window_seconds

        metrics: list[WindowMetrics] = []

        # Group by window bucket and region
        grouped = df.groupby(["window_bucket", "region"])
        for (bucket, region), group in grouped:
            start_dt = datetime.fromtimestamp(bucket, tz=UTC).isoformat()
            end_dt = datetime.fromtimestamp(bucket + self.window_seconds, tz=UTC).isoformat()

            unique_aircraft = group["icao24"].nunique()
            total_obs = len(group)

            valid_alt = group["altitude"].dropna()
            avg_alt = float(valid_alt.mean()) if not valid_alt.empty else 0.0

            valid_vel = group["velocity"].dropna()
            avg_vel = float(valid_vel.mean()) if not valid_vel.empty else 0.0
            max_vel = float(valid_vel.max()) if not valid_vel.empty else 0.0

            valid_vr = group["vertical_rate"].dropna()
            climbing = int((valid_vr > 1.5).sum())
            descending = int((valid_vr < -1.5).sum())
            level_flight = int(((valid_vr >= -1.5) & (valid_vr <= 1.5)).sum())

            metrics.append(
                WindowMetrics(
                    window_start=start_dt,
                    window_end=end_dt,
                    region=str(region),
                    unique_aircraft_count=int(unique_aircraft),
                    total_observations=total_obs,
                    avg_altitude_m=round(avg_alt, 2),
                    avg_velocity_mps=round(avg_vel, 2),
                    max_velocity_mps=round(max_vel, 2),
                    climbing_count=climbing,
                    descending_count=descending,
                    level_flight_count=level_flight,
                )
            )

        # Sort by total_observations descending
        metrics.sort(key=lambda m: m.total_observations, reverse=True)
        return metrics
