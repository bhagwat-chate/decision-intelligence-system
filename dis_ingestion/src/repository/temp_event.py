dis_ingestion_sqs_event = {
    "Records": [
        {
            "messageId": "b7e1c9f4-2c4a-4c11-9d58-1d92b8a0e3aa",
            "receiptHandle": "AQEBzJX3ExampleReceiptHandle==",

            "body": "{\n"
                    "  \"Records\": [\n"
                    "    {\n"
                    "      \"eventVersion\": \"2.1\",\n"
                    "      \"eventSource\": \"aws:s3\",\n"
                    "      \"awsRegion\": \"ap-south-1\",\n"
                    "      \"eventTime\": \"2025-12-23T06:40:00.000Z\",\n"
                    "      \"eventName\": \"ObjectCreated:Put\",\n"
                    "      \"s3\": {\n"
                    "        \"bucket\": {\n"
                    "          \"name\": \"escs-dis-signal-artifacts-dev\",\n"
                    "          \"arn\": \"arn:aws:s3:::escs-dis-signal-artifacts-dev\"\n"
                    "        },\n"
                    "        \"object\": {\n"
                    "          \"key\": \"sales/enterprise_sales_apac/2025/12/23/escs_evt_20251223_0001.json\",\n"
                    "          \"size\": 1842,\n"
                    "          \"eTag\": \"9c8f2e1a5c7a9b1d4f3a6e8b0c7d2e91\"\n"
                    "        }\n"
                    "      }\n"
                    "    }\n"
                    "  ]\n"
                    "}",

            # AWS-managed attributes
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1734938400000",
                "SenderId": "AROAXXXXXXXXXXXXXXXX",
                "ApproximateFirstReceiveTimestamp": "1734938401000"
            },

            # Optional message attributes (routing / observability)
            "messageAttributes": {
                "event_type": {
                    "stringValue": "escs_signal_artifact_created",
                    "dataType": "String"
                },
                "schema_version": {
                    "stringValue": "v1",
                    "dataType": "String"
                },
                "environment": {
                    "stringValue": "dev",
                    "dataType": "String"
                }
            },

            "md5OfBody": "c1d8f89a4f1b2e90c5d6a7b8e9f01234",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:ap-south-1:123456789012:dis-s3-events-dev",
            "awsRegion": "ap-south-1"
        }
    ]
}
