# dis_ingestion/main.py

from typing import Any, Dict

from dis_ingestion.src.config.settings import get_settings
from dis_ingestion.src.logger.logging_config import logger
from dis_ingestion.src.exception.custom_exception import IngestionException
from dis_ingestion.src.schema.sqs_event_schema import validate_sqs_event_schema
from dis_ingestion.src.orchestration.ingestion_orchestrator import IngestionOrchestrator

settings = get_settings()


def ingestion_lambda_handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """
    Lambda entrypoint for DIS ingestion.

    Responsibilities:
    - Enforce SQS/S3 event envelope schema
    - Delegate orchestration to ingestion layer
    - Let AWS manage retries and DLQ
    """

    logger.info("DIS ingestion Lambda invoked", extra={
            "aws_request_id": getattr(context, "aws_request_id", None),
            "record_count": len(event.get("Records", [])),
            "env": settings.DIS_ENV,
        },)

    # Schema enforcement (transport-level only)
    validate_sqs_event_schema(event)

    logger.info("SQS event schema validation succeeded", extra={
            "stage": "transport_schema_validation",
            "event_source": "sqs",
            "record_count": len(event.get("Records", [])),
            "validation_status": "passed",
            "env": settings.DIS_ENV,
        },)

    logger.info("Starting ingestion orchestration", extra={
            "stage": "orchestration_start",
            "record_count": len(event.get("Records", [])),
            "env": settings.DIS_ENV,
        },)

    # Orchestration (business flow)
    IngestionOrchestrator().orchestrate(event)

    logger.info("DIS ingestion Lambda completed successfully", extra={"env": settings.DIS_ENV},)

    return {"status": "success"}
