# dis_ingestion/src/orchestration/ingestion_orchestrator.py

from typing import Dict, Any

from dis_ingestion.src.logger.logging_config import logger
from dis_ingestion.src.exception.custom_exception import IngestionException

from dis_ingestion.src.executors.s3_artifact_fetcher import S3ArtifactFetcher
from dis_ingestion.src.executors.escs_contract_validator import ESCSContractValidator
from dis_ingestion.src.executors.canonicalizer import SignalCanonicalizer
from dis_ingestion.src.executors.persistence_executor import CanonicalArtifactPersister


class IngestionOrchestrator:
    """
    Controller responsible for coordinating the DIS ingestion flow.

    This class:
    - defines the ingestion sequence
    - wires executor components
    - enforces orchestration boundaries

    It does NOT:
    - fetch data itself
    - validate schemas directly
    - persist data
    """

    def __init__(self):
        self.fetcher = S3ArtifactFetcher()
        self.validator = ESCSContractValidator()
        self.canonicalizer = SignalCanonicalizer()
        self.persister = CanonicalArtifactPersister()

    def orchestrate(self, event: Dict[str, Any]) -> None:
        """
        Entry point for end-to-end ingestion orchestration.
        """
        try:
            records = event.get("Records", [])

            logger.info("Ingestion orchestration started", extra={"stage": "orchestration_start", "record_count": len(records),},)

            for sqs_index, record in enumerate(records):
                self._orchestrate_sqs_record(record, sqs_index)

            logger.info("Ingestion orchestration completed", extra={"stage": "orchestration_complete", "record_count": len(records),},)

        except Exception as exc:
            logger.error("Ingestion orchestration failed", extra={"stage": "orchestration_error", "error": str(exc),},)
            raise IngestionException("Ingestion orchestration failed") from exc

    def _orchestrate_sqs_record(self, record: Dict[str, Any], sqs_index: int) -> None:
        """
        Orchestrates ingestion for a single SQS record.
        """
        try:
            logger.info("Orchestrating SQS record", extra={"stage": "orchestration_sqs", "sqs_record_index": sqs_index,},)

            escs_payload = self._fetch_escs_artifact(record)
            self._validate_contract(escs_payload)
            canonical_payload = self._canonicalize(escs_payload)
            self._persist(canonical_payload)

            logger.info("SQS record orchestration succeeded", extra={"stage": "orchestration_sqs_success", "sqs_record_index": sqs_index,},)

        except Exception as exc:
            logger.error("SQS record orchestration failed", extra={"stage": "orchestration_sqs_error", "sqs_record_index": sqs_index, "error": str(exc),},)
            raise
