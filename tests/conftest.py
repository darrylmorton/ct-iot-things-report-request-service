import os

import boto3
import pytest
from moto import mock_sqs

from config import THINGS_REPORT_REQUEST_QUEUE
from things_report_request_service.service import log


# from .helper.request_helper import sqs


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def sqs_client(aws_credentials):
    with mock_sqs():
        client = boto3.client("sqs", region_name="eu-west-2")

        queue = client.create_queue(
            QueueName=f"{THINGS_REPORT_REQUEST_QUEUE}.fifo",
            # Attributes={'DelaySeconds': '5'}
        )
        log.info(f"**** queue {queue}")
        yield queue
        #
        # with pytest.raises(ClientError) as exc:
        #     log.info(f"**** exc {exc}")

        # try:
        #     queue.send_messages(
        #         Entries=[{
        #             "Id": "1",
        #             "MessageAttributes": {
        #                 "Thing": {
        #                     "StringValue": "ThingOne",
        #                     "DataType": "String"
        #                 },
        #                 'ThingType': {
        #                     'StringValue': 'ThingTypeOne',
        #                     'DataType': 'String'
        #                 },
        #                 'StartDate': {
        #                     'StringValue': 'DayOne',
        #                     'DataType': 'String'
        #                 },
        #                 'EndDate': {
        #                     'StringValue': 'DayOne',
        #                     'DataType': 'String'
        #                 }
        #             },
        #             "MessageBody": json.dumps({
        #                 "Thing": "ThingOne",
        #                 "ThingType": "ThingTypeOne",
        #                 "StartDate": "DayOne",
        #                 "EndDate": "DayOne",
        #             }),
        #             "MessageDeduplicationId": "1",
        #         }]
        #     )
        # except Exception as err:
        #     log.info(f"**** err {err}")

# @pytest.fixture(scope='function')
# def sqs_client(aws):
#     client = boto3.client("sqs")
#     client.create_queue(QueueName=THINGS_REPORT_REQUEST_QUEUE)
#
#     client.send_message(messages)


# @mock_sqs
# def mock_sqs_producer():
#     # conn = boto3.resource("sqs", region_name="eu-west-2")
#     # conn.create_queue(QueueName=THINGS_REPORT_REQUEST_QUEUE)
#
#     conn.send_message(request_helper.messages)


# @pytest.fixture
# def create_bucket2(aws):
#     boto3.client("s3").create_bucket(Bucket="b2")


# def test_s3_directly(aws):
#     s3.create_bucket(Bucket="somebucket")
#     result = s3.list_buckets()
#     assert len(result["Buckets"]) == 1


# def test_bucket_creation(create_bucket1, create_bucket2):
#     buckets = boto3.client("s3").list_buckets()["Buckets"]
#     assert len(result["Buckets"]) == 2
