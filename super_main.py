from dis_ingestion.main import ingestion_lambda_handler
from dis_ingestion.src.repository.temp_event import dis_ingestion_sqs_event


if __name__ == '__main__':

    ingestion_lambda_handler(dis_ingestion_sqs_event)
