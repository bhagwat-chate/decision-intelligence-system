# dis_ingestion/src/executors/s3_artifact_fetcher.py

import json
from typing import Dict, Any

import boto3

from dis_ingestion.src.logger.logging_config import logger
from dis_ingestion.src.exception.custom_exception import IngestionException


class S3ArtifactFetcher:
    """
    Executor responsible for fetching ESCS artifacts from S3.
    """

    def __init__(self, s3_client=None):
        self._s3_client = s3_client or boto3.client("s3")

    def fetch(self, record: Dict[str, Any]) -> Dict[str, Any]:
        try:
            s3_info = record["body"]
            s3_event = json.loads(s3_info)
            s3_record = s3_event["Records"][0]["s3"]

            bucket = s3_record["bucket"]["name"]
            key = s3_record["object"]["key"]

            logger.info("Fetching ESCS artifact from S3", extra={"stage": "s3_fetch", "bucket": bucket, "object_key": key,},)

            response = self._s3_client.get_object(Bucket=bucket, Key=key)
            payload = json.loads(response["Body"].read())

            logger.info("ESCS artifact fetched successfully", extra={
                    "stage": "s3_fetch_success",
                    "bucket": bucket,
                    "object_key": key,
                },)

            return payload

        except Exception as exc:
            logger.error("Failed to fetch ESCS artifact from S3", extra={"stage": "s3_fetch_error", "error": str(exc), },)
            raise IngestionException("S3 artifact fetch failed") from exc
