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

        report_request_queue.send_messages(Entries=expected_message_batch_one)
        service_poll(request_service, 10)

        actual_job_messages_batch_one = report_jobs_consumer(report_job_queue, 20)

        assert_job_messages(actual_job_messages_batch_one, expected_job_message_batch_one)
