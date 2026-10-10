"""Telemetry schema definitions and validation models."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class AircraftTelemetry(BaseModel):
    """Normalized Aircraft Telemetry message adhering to ADS-B standard."""

    icao24: str = Field(..., description="Unique ICAO 24-bit address of the transponder in hex")
    callsign: str | None = Field(default=None, description="Callsign of the vehicle (8 chars)")
    origin_country: str = Field(
        ..., description="Country name inferred from the ICAO 24-bit address"
    )
    time_position: int | None = Field(
        default=None, description="Unix timestamp (seconds) for the last position update"
    )
    last_contact: int = Field(
        ..., description="Unix timestamp (seconds) for the last update in general"
    )
    longitude: float | None = Field(
        default=None, ge=-180.0, le=180.0, description="WGS-84 longitude in decimal degrees"
    )
    latitude: float | None = Field(
        default=None, ge=-90.0, le=90.0, description="WGS-84 latitude in decimal degrees"
    )
    baro_altitude: float | None = Field(default=None, description="Barometric altitude in meters")
    on_ground: bool = Field(default=False, description="True if aircraft is on ground")
    velocity: float | None = Field(default=None, ge=0.0, description="Velocity over ground in m/s")
    true_track: float | None = Field(
        default=None,
        ge=0.0,
        le=360.0,
        description="True track in decimal degrees clockwise from north",
    )
    vertical_rate: float | None = Field(
        default=None, description="Vertical rate in m/s. Positive=climbing, negative=descending"
    )
    geo_altitude: float | None = Field(default=None, description="Geometric altitude in meters")
    squawk: str | None = Field(default=None, description="Transponder code (squawk)")
    spi: bool = Field(default=False, description="Special purpose indicator")
    position_source: int = Field(default=0, description="0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM")
    ingested_at: str = Field(
        default_factory=lambda: datetime.now(UTC).isoformat(),
        description="ISO 8601 ingestion timestamp",
    )

    @field_validator("icao24")
    @classmethod
    def clean_icao24(cls, v: str) -> str:
        v_clean = v.strip().lower()
        if not v_clean:
            raise ValueError("icao24 cannot be empty")
        return v_clean

    @field_validator("callsign")
    @classmethod
    def clean_callsign(cls, v: str | None) -> str | None:
        if v is None:
            return None
        cleaned = v.strip()
        return cleaned if cleaned else None

    @property
    def velocity_kmh(self) -> float | None:
        """Ground velocity converted to km/h."""
        if self.velocity is not None:
            return round(self.velocity * 3.6, 2)
        return None

    @classmethod
    def from_opensky_vector(cls, vector: list[Any]) -> AircraftTelemetry | None:
        """Parse raw OpenSky API state vector array into validated AircraftTelemetry.

        Returns None if vector format is invalid.
        """
        if not isinstance(vector, (list, tuple)) or len(vector) < 17:
            return None

        try:
            return cls(
                icao24=str(vector[0]),
                callsign=str(vector[1]) if vector[1] is not None else None,
                origin_country=str(vector[2]) if vector[2] is not None else "Unknown",
                time_position=int(vector[3]) if vector[3] is not None else None,
                last_contact=int(vector[4])
                if vector[4] is not None
                else int(datetime.now(UTC).timestamp()),
                longitude=float(vector[5]) if vector[5] is not None else None,
                latitude=float(vector[6]) if vector[6] is not None else None,
                baro_altitude=float(vector[7]) if vector[7] is not None else None,
                on_ground=bool(vector[8]) if vector[8] is not None else False,
                velocity=float(vector[9]) if vector[9] is not None else None,
                true_track=float(vector[10]) if vector[10] is not None else None,
                vertical_rate=float(vector[11]) if vector[11] is not None else None,
                geo_altitude=float(vector[13]) if vector[13] is not None else None,
                squawk=str(vector[14]) if vector[14] is not None else None,
                spi=bool(vector[15]) if vector[15] is not None else False,
                position_source=int(vector[16]) if vector[16] is not None else 0,
            )
        except (ValueError, TypeError):
            return None


class DeadLetterPayload(BaseModel):
    """Payload stored when a raw telemetry message fails validation."""

    raw_data: Any
    reason: str
    failed_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
