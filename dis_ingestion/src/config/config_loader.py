# dis_ingestion/src/config/config_loader.py

import json
import boto3
from typing import Dict

from dis_ingestion.src.config.settings import get_settings
from dis_ingestion.src.logger.logging_config import logger


def load_llm_secrets() -> Dict[str, str]:
    """
    Load LLM secrets from AWS Secrets Manager.
    Used ONLY in production (Lambda / ECS).
    """

    settings = get_settings()

    secret_name = "ingestion-layer-secrets"
    region_name = settings.AWS_SECRET_REGION

    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_str = response["SecretString"]
        secrets = json.loads(secret_str)

        logger.info("ingestion secrets loaded from Secrets Manager loaded")

        return secrets

    except Exception as e:
        logger.error("Failed to load LLM secrets", secret_name=secret_name, error=str(e))
        raise