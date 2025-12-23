# dis_ingestion/src/config/settings.py

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv


# Resolve service root explicitly
DIS_INGESTION_ROOT = Path(__file__).resolve().parents[2]

# Load .env explicitly for local dev (safe, ignored in prod Lambda)
load_dotenv(dotenv_path=DIS_INGESTION_ROOT / ".env", override=False)


class Settings(BaseSettings):
    """
    Global settings for DIS Ingestion (Lambda-based, event-driven).

    Responsibilities:
    - Triggered ONLY by SQS events originating from S3 notifications
    - Fetch ESCS-curated signal artifacts from S3
    - Validate ESCS → DIS JSON contract
    - Persist raw + canonical artifacts
    - Emit ingestion metadata (logs, metrics, audit hooks)

    """

    DIS_ENV: str = "dev"
    AWS_REGION: str = "ap-south-1"
    ESCS_SIGNAL_BUCKET_NAME: str
    ESCS_SIGNAL_BUCKET_PREFIX: str = ""  # Optional safety filter (e.g., sales/)
    CANONICAL_ARTIFACT_BUCKET_NAME: str
    CANONICAL_ARTIFACT_PREFIX: str = "canonical"
    S3_EVENT_QUEUE_ARN: str  # Used for IAM / observability reference only
    ENABLE_SCHEMA_VALIDATION: bool = True
    ENABLE_IDEMPOTENCY_CHECK: bool = True
    ENABLE_DLQ_FORWARDING: bool = True

    LOG_LEVEL: str = "INFO"
    ENABLE_FILE_LOGGING: bool = False  # Typically false for Lambda

    model_config = SettingsConfigDict(env_file=DIS_INGESTION_ROOT / ".env", env_file_encoding="utf-8", extra="forbid")


@lru_cache()
def get_settings() -> Settings:
    """
    Cached settings accessor.
    Safe for Lambda cold starts.
    """
    return Settings()
