import logging
import time

import boto3
from moto import mock_sqs

from config import THINGS_REPORT_REQUEST_QUEUE
from things_report_request_service.service import ThingsReportRequestService
from ..helper.request_helper import create_messages

log = logging.getLogger("service")


# @pytest.mark.skip
# @mock_sqs
# def mock_sqs_producer(sqs_client):
#     sqs_client.send_messages(
#         Entries=[{
#             'Id': '1',
#             'MessageBody': "1",
#             'MessageAttributes': {
#                 'Thing': {
#                     'StringValue': 'ThingOne',
#                     'DataType': 'String'
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
#             }
#             # }
#         },
#             {
#                 'Id': '2',
#                 'MessageBody': "2",  # {
#                 'MessageAttributes': {
#                     'Thing': {
#                         'StringValue': 'ThingOne',
#                         'DataType': 'String'
#                     },
#                     'ThingType': {
#                         'StringValue': 'ThingTypeOne',
#                         'DataType': 'String'
#                     },
#                     'StartDate': {
#                         'StringValue': 'DayOne',
#                         'DataType': 'String'
#                     },
#                     'EndDate': {
#                         'StringValue': 'DayOne',
#                         'DataType': 'String'
#                     }
#                 }
#                 # }
#             }])
#
#     messages = sqs_client.receive_messages(MaxNumberOfMessages=2)
#     log.info(f"**** messages {messages}")

# conn = sqs_client("sqs", region_name=AWS_DEFAULT_REGION)
# conn.create_queue(QueueName=THINGS_REPORT_REQUEST_QUEUE)

# conn.send_messages(
#     QueueUrl="THINGS_REPORT_REQUEST_QUEUE",
#     Entries=[
#         {
#             'Id': '1',
#             'MessageBody': '1',
#             'MessageAttributes': {
#                 'Thing': {
#                     'StringValue': 'ThingOne',
#                     'DataType': 'String'
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
#             }
#         },
#         {
#             'Id': '2',
#             'MessageBody': '2',
#             'MessageAttributes': {
#                 'Thing': {
#                     'StringValue': 'ThingOne',
#                     'DataType': 'String'
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
#             }
#         }
#     ])

# service.run()


@mock_sqs
def test_request_consumer():
    sqs = boto3.resource("sqs", region_name="eu-west-2")
    log.info(f"**** sqs {sqs}")

    queue = sqs.create_queue(
        QueueName=f"{THINGS_REPORT_REQUEST_QUEUE}.fifo",
        Attributes={"DelaySeconds": "5"}
    )
    log.info(f"**** queue {queue}")

    request_service = ThingsReportRequestService()
    request_service.start()
    log.info(f"**** test_request_consumer running...")

    # with pytest.raises(ClientError) as exc:
    #     log.info(f"**** exc {exc}")
    message_batch_one = create_messages(10)
    # message_batch_two = create_messages(10, 10)
    # message_batch_three = create_messages(10, 20)

    try:
        log.info(f"**** message_batch_one {message_batch_one}")
        queue.send_messages(Entries=message_batch_one)
        time.sleep(5)

        # log.info(f"**** message_batch_two {message_batch_two}")
        # queue.send_messages(Entries=message_batch_two)
        # time.sleep(5)
        #
        # log.info(f"**** message_batch_three {message_batch_three}")
        # queue.send_messages(Entries=message_batch_three)
        # time.sleep(5)

    except Exception as err:
        log.error(f"**** err {err}")
