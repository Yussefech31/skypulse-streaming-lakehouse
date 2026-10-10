"""OpenSky Network public API client for ADS-B live data ingestion."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from config.config import settings
from src.ingestion.schema import AircraftTelemetry

logger = logging.getLogger(__name__)


class OpenSkyClient:
    """HTTP client for OpenSky Network state vectors with resilient snapshot fallback."""

    BASE_URL = "https://opensky-network.org/api/states/all"

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        snapshot_path: Path | None = None,
        timeout: int = 15,
    ) -> None:
        self.username = username or settings.opensky_username
        self.password = password or settings.opensky_password
        self.bbox = bbox
        self.snapshot_path = snapshot_path or settings.sample_snapshot_path
        self.timeout = timeout
        self.session = requests.Session()
        if self.username and self.password:
            self.session.auth = HTTPBasicAuth(self.username, self.password)

    def _build_params(self) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if self.bbox:
            lamin, lomin, lamax, lomax = self.bbox
            params.update(
                {
                    "lamin": lamin,
                    "lomin": lomin,
                    "lamax": lamax,
                    "lomax": lomax,
                }
            )
        return params

    def fetch_states_raw(self) -> dict[str, Any]:
        """Fetch raw state vectors from live API; fall back to local snapshot if error."""
        try:
            params = self._build_params()
            response = self.session.get(self.BASE_URL, params=params, timeout=self.timeout)
            if response.status_code == 200:
                data = response.json()
                logger.info(
                    "Fetched %d live aircraft states from OpenSky API",
                    len(data.get("states", [])),
                )
                return data
            logger.warning(
                "OpenSky API returned HTTP %d: %s. Using local snapshot fallback.",
                response.status_code,
                response.text[:200],
            )
        except requests.RequestException as e:
            logger.warning("OpenSky API connection failed: %s. Falling back to snapshot.", e)

        return self.load_local_snapshot()

    def load_local_snapshot(self) -> dict[str, Any]:
        """Load locally cached real OpenSky snapshot."""
        if not self.snapshot_path.exists():
            raise FileNotFoundError(f"Snapshot file not found at {self.snapshot_path}")

        logger.info("Loading cached OpenSky snapshot from %s", self.snapshot_path)
        with open(self.snapshot_path, encoding="utf-8") as f:
            return json.load(f)

    def fetch_telemetry_batch(self) -> list[AircraftTelemetry]:
        """Fetch and validate a batch of telemetry records."""
        payload = self.fetch_states_raw()
        states = payload.get("states", [])
        if not states:
            return []

        validated: list[AircraftTelemetry] = []
        for vec in states:
            record = AircraftTelemetry.from_opensky_vector(vec)
            if record is not None:
                validated.append(record)

        return validated
