# dis_ingestion/src/logger/logging_config.py

from dis_ingestion.src.logger.custom_logger import CustomLogger

logger = CustomLogger().get_logger("cardio_ingestion")