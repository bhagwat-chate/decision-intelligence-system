# dis_ingestion/src/executors/persistence_executor.py

import json
from typing import Dict, Any
from datetime import datetime

import boto3

from dis_ingestion.src.config.settings import get_settings
from dis_ingestion.src.logger.logging_config import logger
from dis_ingestion.src.exception.custom_exception import IngestionException


class CanonicalArtifactPersister:
    """
    Executor responsible for persisting canonical DIS artifacts.
    """

    def __init__(self, s3_client=None):
        self._settings = get_settings()
        self._s3_client = s3_client or boto3.client("s3", region_name=self._settings.AWS_REGION)

    def persist(self, canonical_payload: Dict[str, Any]) -> None:
        try:
            event_meta = canonical_payload["event_meta"]
            source_context = canonical_payload["source_context"]

            object_key = self._build_object_key(event_meta, source_context)

            logger.info("Persisting canonical artifact", extra={
                    "stage": "persistence_start",
                    "bucket": self._settings.CANONICAL_ARTIFACT_BUCKET_NAME,
                    "object_key": object_key,
                },)

            self._s3_client.put_object(
                Bucket=self._settings.CANONICAL_ARTIFACT_BUCKET_NAME,
                Key=object_key,
                Body=json.dumps(canonical_payload),
                ContentType="application/json",
            )

            logger.info("Canonical artifact persisted successfully", extra={
                    "stage": "persistence_complete",
                    "bucket": self._settings.CANONICAL_ARTIFACT_BUCKET_NAME,
                    "object_key": object_key,
                },)

        except Exception as exc:
            logger.error("Canonical artifact persistence failed", extra={"stage": "persistence_error", "error": str(exc),},)
            raise IngestionException("Canonical artifact persistence failed") from exc

    def _build_object_key(
        self, event_meta: Dict[str, Any], source_context: Dict[str, Any]
    ) -> str:
        """
        Build deterministic S3 object key for canonical artifact.
        """
        try:
            date_path = datetime.utcnow().strftime("%Y/%m/%d")

            return (
                f"{self._settings.CANONICAL_ARTIFACT_PREFIX}/"
                f"{source_context.get('business_domain')}/"
                f"{date_path}/"
                f"{event_meta.get('event_id')}.json"
            )

        except Exception as exc:
            logger.error("Failed to build canonical artifact object key", extra={"stage": "persistence_key_error", "error": str(exc),},)
            raise
