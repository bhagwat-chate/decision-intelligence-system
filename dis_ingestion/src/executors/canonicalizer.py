# dis_ingestion/src/executors/canonicalizer.py

from typing import Dict, Any

from dis_ingestion.src.logger.logging_config import logger
from dis_ingestion.src.exception.custom_exception import IngestionException


class SignalCanonicalizer:
    """
    Executor responsible for converting ESCS payload
    into DIS canonical signal format.
    """

    def canonicalize(self, escs_payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            logger.info("Canonicalization started", extra={
                    "stage": "canonicalization_start",
                    "schema_version": escs_payload.get("event_meta", {}).get("schema_version"),
                },)

            canonical_payload = {
                "event_meta": escs_payload["event_meta"],
                "source_context": escs_payload["source_context"],
                "signal_payload": escs_payload["signal_payload"],
            }

            logger.info("Canonicalization completed", extra={
                    "stage": "canonicalization_complete",
                    "signal_count": len(
                        canonical_payload.get("signal_payload", {}).get("signals", [])
                    ),},)

            return canonical_payload

        except Exception as exc:
            logger.error("Canonicalization failed", extra={
                    "stage": "canonicalization_error",
                    "error": str(exc),},)
            raise IngestionException("Signal canonicalization failed") from exc
