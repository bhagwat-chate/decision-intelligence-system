# dis_ingestion/src/schema/sqs_event_schema.py

import json
from typing import Dict, Any, List

from dis_ingestion.src.exception.custom_exception import IngestionException
from dis_ingestion.src.logger.logging_config import logger


def validate_sqs_event_schema(event: Dict[str, Any]) -> None:
    """
    Validate the SQS event envelope delivered to Lambda.

    Scope:
    - Validate AWS SQS structure
    - Validate S3 event notification structure inside SQS body
    - Ensure bucket and object key are present

    This function enforces TRANSPORT SCHEMA only.
    It does NOT:
    - fetch S3 objects
    - validate ESCS → DIS JSON contract
    - interpret business meaning
    """

    if not isinstance(event, dict):
        raise IngestionException("Lambda event must be a dictionary")

    records = event.get("Records")
    if not records or not isinstance(records, list):
        raise IngestionException("Missing or invalid 'Records' in SQS event")

    for idx, record in enumerate(records):
        _validate_single_sqs_record(record, idx)


def _validate_single_sqs_record(record: Dict[str, Any], index: int) -> None:
    """
    Validate a single SQS record wrapper.
    """

    if "body" not in record:
        raise IngestionException(f"SQS record[{index}] missing 'body' field")

    body = record["body"]

    try:
        s3_event = json.loads(body)
    except json.JSONDecodeError as exc:
        raise IngestionException(
            f"SQS record[{index}] body is not valid JSON"
        ) from exc

    _validate_s3_event_schema(s3_event, index)


def _validate_s3_event_schema(s3_event: Dict[str, Any], index: int) -> None:
    """
    Validate the S3 event notification schema.
    """

    records = s3_event.get("Records")
    if not records or not isinstance(records, list):
        raise IngestionException(
            f"SQS record[{index}] does not contain valid S3 'Records'"
        )

    for rec_idx, s3_record in enumerate(records):
        _validate_single_s3_record(s3_record, index, rec_idx)


def _validate_single_s3_record(
    s3_record: Dict[str, Any],
    sqs_index: int,
    s3_index: int
) -> None:
    """
    Validate a single S3 event record.
    """

    if "s3" not in s3_record:
        raise IngestionException(
            f"SQS record[{sqs_index}] S3 record[{s3_index}] missing 's3' block"
        )

    s3_block = s3_record["s3"]

    bucket = s3_block.get("bucket", {}).get("name")
    obj_key = s3_block.get("object", {}).get("key")

    if not bucket:
        raise IngestionException(
            f"SQS record[{sqs_index}] S3 record[{s3_index}] missing bucket name"
        )

    if not obj_key:
        raise IngestionException(
            f"SQS record[{sqs_index}] S3 record[{s3_index}] missing object key"
        )

    logger.debug(
        "Validated S3 event record",
        extra={
            "bucket": bucket,
            "key": obj_key,
            "sqs_record_index": sqs_index,
            "s3_record_index": s3_index,
        },
    )
