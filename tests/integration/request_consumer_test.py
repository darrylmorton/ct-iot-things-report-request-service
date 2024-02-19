import logging

from moto import mock_sqs

from config import THINGS_REPORT_REQUEST_QUEUE, THINGS_REPORT_JOB_QUEUE
from things_report_request_service.service import ThingsReportRequestService
from ..helper.request_helper import (
    create_messages, service_poll, create_sqs_queue, report_jobs_consumer, expected_job_messages, assert_job_messages
)

log = logging.getLogger("test_things_report_request_service")


class TestRequestService:
    @mock_sqs
    def test_request_consumer(self):
        request_service = ThingsReportRequestService()
        report_request_queue = create_sqs_queue(THINGS_REPORT_REQUEST_QUEUE)
        report_job_queue = create_sqs_queue(THINGS_REPORT_JOB_QUEUE)

        expected_message_batch_one = create_messages(1)
        expected_job_message_batch_one = expected_job_messages(expected_message_batch_one)
        log.info(f"TEST len(expected_job_message_batch_one) {len(expected_job_message_batch_one)}")
        # log.info(f"TEST expected_job_message_batch_one {expected_job_message_batch_one}")

        report_request_queue.send_messages(Entries=expected_message_batch_one)
        service_poll(request_service, 10)
        # log.info(f"TEST actual_message_batch_one {actual_message_batch_one}")

        # assert_request_messages(actual_message_batch_one, expected_message_batch_one)

        # expected_message_batch_two = create_messages(10, 10)
        # report_request_queue.send_messages(Entries=expected_message_batch_two)
        # actual_message_batch_two = service_poll(request_service, 10)
        # assert_request_messages(actual_message_batch_two, expected_message_batch_two)
        #
        # expected_message_batch_three = create_messages(5, 20)
        # report_request_queue.send_messages(Entries=expected_message_batch_three)
        # actual_messages_batch_three = service_poll(request_service, 10)
        # assert_request_messages(actual_messages_batch_three, expected_message_batch_three)

        # request_service.produce()
        # report_job_queue = request_service.report_job_queue
        actual_job_messages_batch_one = report_jobs_consumer(report_job_queue, 20)
        log.info(f"TEST len(actual_job_messages_batch_one) {len(actual_job_messages_batch_one)}")
        # log.info(f"TEST actual_job_messages_batch_one {actual_job_messages_batch_one}")

        assert_job_messages(actual_job_messages_batch_one, expected_job_message_batch_one)

        # job_messages = report_job_queue.receive_messages(
        #     MessageAttributeNames=["All"],
        #     MaxNumberOfMessages=10,
        #     WaitTimeSeconds=5,
        # )
        # log.info(f"job_messages {job_messages}")
