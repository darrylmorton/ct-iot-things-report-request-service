import logging

from moto import mock_sqs

from config import THINGS_REPORT_REQUEST_QUEUE, THINGS_REPORT_JOB_QUEUE
from things_report_request_service.service import ThingsReportRequestService
from ..helper.request_helper import create_messages, service_poll, create_sqs_queue, service_consume

log = logging.getLogger("test_things_report_request_service")


class TestRequestService:
    @mock_sqs
    def test_request_consumer(self):
        request_service = ThingsReportRequestService()
        report_request_queue = create_sqs_queue(THINGS_REPORT_REQUEST_QUEUE)
        report_job_queue = create_sqs_queue(THINGS_REPORT_JOB_QUEUE)

        message_batch_one = create_messages(10)
        # log.info(f"**** message_batch_one {message_batch_one}")
        log.info(f"**** len(message_batch_one) {len(message_batch_one)}")

        report_request_queue.send_messages(Entries=message_batch_one)
        service_poll(request_service, 10)

        message_batch_two = create_messages(10, 10)
        # log.info(f"**** message_batch_two {message_batch_two}")
        log.info(f"**** len(message_batch_two) {len(message_batch_two)}")

        report_request_queue.send_messages(Entries=message_batch_two)
        service_poll(request_service, 10)

        message_batch_three = create_messages(5, 20)
        log.info(f"**** message_batch_three {message_batch_three}")

        report_request_queue.send_messages(Entries=message_batch_three)
        service_poll(request_service, 10)

        service_consume(report_job_queue, 10)

        # job_messages = report_job_queue.receive_messages(
        #     MessageAttributeNames=["All"],
        #     MaxNumberOfMessages=10,
        #     WaitTimeSeconds=5,
        # )
        # log.info(f"job_messages {job_messages}")
