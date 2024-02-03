import logging

from moto import mock_sqs

from config import THINGS_REPORT_REQUEST_QUEUE
from things_report_request_service.service import ThingsReportRequestService
from ..helper.request_helper import create_messages, service_poll, create_sqs_queue

log = logging.getLogger("test_things_report_request_service")


class TestRequestService:
    @mock_sqs
    def test_request_consumer(self):
        request_service = ThingsReportRequestService()
        queue = create_sqs_queue(THINGS_REPORT_REQUEST_QUEUE)

        message_batch_one = create_messages(10)
        log.info(f"**** message_batch_one {message_batch_one}")

        queue.send_messages(Entries=message_batch_one)
        service_poll(request_service, 5)

        message_batch_two = create_messages(10, 10)
        log.info(f"**** message_batch_two {message_batch_two}")

        queue.send_messages(Entries=message_batch_two)
        service_poll(request_service, 5)

        message_batch_three = create_messages(5, 20)
        log.info(f"**** message_batch_three {message_batch_three}")

        queue.send_messages(Entries=message_batch_three)
        service_poll(request_service, 10)
