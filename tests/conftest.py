import os

import pytest


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

# @pytest.fixture(scope="function")
# async def sqs_client(aws_credentials):
#     with mock_sqs():
#         client = boto3.client("sqs", region_name="eu-west-2")
#
#         queue = client.create_queue(
#             QueueName=f"{THINGS_REPORT_REQUEST_QUEUE}.fifo",
#             # Attributes={'DelaySeconds': '5'}
#         )
#         log.info(f"**** queue {queue}")
#
#         yield queue
