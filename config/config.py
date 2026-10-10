"""Configuration management for SkyPulse Streaming Lakehouse."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load .env if present
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    # OpenSky Network API
    opensky_username: str = os.getenv("OPENSKY_USERNAME", "")
    opensky_password: str = os.getenv("OPENSKY_PASSWORD", "")
    opensky_bbox_lamin: float = float(os.getenv("OPENSKY_BBOX_LAMIN", "27.5"))
    opensky_bbox_lomin: float = float(os.getenv("OPENSKY_BBOX_LOMIN", "-14.0"))
    opensky_bbox_lamax: float = float(os.getenv("OPENSKY_BBOX_LAMAX", "36.5"))
    opensky_bbox_lomax: float = float(os.getenv("OPENSKY_BBOX_LOMAX", "-1.0"))
    ingestion_interval_seconds: int = int(os.getenv("INGESTION_INTERVAL_SECONDS", "10"))
    sample_snapshot_path: Path = BASE_DIR / "data" / "sample_opensky_snapshot.json"

    # Kafka
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9096")
    kafka_topic_telemetry: str = os.getenv("KAFKA_TOPIC_TELEMETRY", "aircraft-telemetry-raw")
    kafka_topic_anomalies: str = os.getenv("KAFKA_TOPIC_ANOMALIES", "aircraft-anomalies")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "skypulse-lakehouse-group")

    # MinIO / S3 Lakehouse
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9005")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    lakehouse_bucket_silver: str = os.getenv("LAKEHOUSE_BUCKET_SILVER", "lakehouse-silver")
    lakehouse_bucket_gold: str = os.getenv("LAKEHOUSE_BUCKET_GOLD", "lakehouse-gold")
    lakehouse_checkpoint_dir: str = os.getenv("LAKEHOUSE_CHECKPOINT_DIR", "lakehouse-checkpoints")
    local_lakehouse_dir: Path = BASE_DIR / "lakehouse"

    # Streaming Parameters
    window_duration_minutes: int = int(os.getenv("WINDOW_DURATION_MINUTES", "5"))
    slide_duration_minutes: int = int(os.getenv("SLIDE_DURATION_MINUTES", "1"))
    watermark_delay_minutes: int = int(os.getenv("WATERMARK_DELAY_MINUTES", "10"))
    speed_change_anomaly_threshold_mps: float = float(
        os.getenv("SPEED_CHANGE_ANOMALY_THRESHOLD_MPS", "25.0")
    )
    vertical_rate_anomaly_threshold_mps: float = float(
        os.getenv("VERTICAL_RATE_ANOMALY_THRESHOLD_MPS", "15.0")
    )

    # Dashboard
    streamlit_server_port: int = int(os.getenv("STREAMLIT_SERVER_PORT", "8501"))


settings = Settings()
