# dis_ingestion/src/executors/escs_contract_validator.py

from typing import Dict, Any

from dis_ingestion.src.logger.logging_config import logger
from dis_ingestion.src.exception.custom_exception import IngestionException


class ESCSContractValidator:
    """
    Executor responsible for validating ESCS → DIS JSON contract.
    """

    def validate(self, payload: Dict[str, Any]) -> None:
        try:
            logger.info("Validating ESCS → DIS contract", extra={"stage": "escs_contract_validation"},)

            if "event_meta" not in payload or "signal_payload" not in payload:
                raise ValueError("Missing mandatory ESCS fields")

            logger.info("ESCS → DIS contract validation succeeded", extra={"stage": "escs_contract_validation_success"},)

        except Exception as exc:
            logger.error("ESCS → DIS contract validation failed", extra={
                    "stage": "escs_contract_validation_error",
                    "error": str(exc),},)
            raise IngestionException("ESCS contract validation failed") from exc
