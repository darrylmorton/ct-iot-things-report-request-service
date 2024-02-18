import logging
from typing import Any

import boto3
from botocore.exceptions import ClientError

from config import THINGS_REPORT_REQUEST_QUEUE, THINGS_REPORT_JOB_QUEUE
from util.service_util import create_job_messages

log = logging.getLogger("things_report_request_service")


class ThingsReportRequestService:
    def __init__(self):
        self.sqs = boto3.resource("sqs", region_name="eu-west-2")
        self.report_request_queue = self.sqs.Queue(f"{THINGS_REPORT_REQUEST_QUEUE}.fifo")
        self.report_job_queue = self.sqs.Queue(f"{THINGS_REPORT_JOB_QUEUE}.fifo")

        log.info(f"report_request_queue {self.report_request_queue}")
        log.info(f"report_job_queue {self.report_job_queue}")

    def poll(self):
        while True:
            request_messages = self.consume()

            if len(request_messages) > 0:
                self.produce(request_messages)

    def consume(self) -> Any:
        try:
            request_messages = self.report_request_queue.receive_messages(
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
            )
            # log.info(f"request_messages {request_messages}")

            total_messages = len(request_messages)
            log.info(f"**** total_messages {total_messages}")

            result = self.produce(request_messages)
            log.info(f"**** service produce result {result}")

            return request_messages
        except ClientError as error:
            log.error("Couldn't receive messages from queue: %s", self.report_request_queue)
            raise error

    def produce(self, request_messages: Any):
        log.info(f"**** produce request_messages {request_messages}")

        try:
            job_messages = create_job_messages(request_messages)
            # log.info(f"**** produce len(job_messages) {len(job_messages)}")

            total_messages = len(job_messages)
            log.info(f"**** produce total_messages {total_messages}")

            # log.info(f"**** job_messages: {job_messages}")
            # log.info(f"**** len(job_messages): {len(job_messages)}")

            if len(job_messages) > 0:
                messages = []
                index = 1

                for message in job_messages:
                    if index == 10:
                        self.report_job_queue.send_messages(Entries=job_messages)
                        index = 1

                    messages.append(message)
                    # log.info(f"**** produce job_messages {job_messages}")

                    # log.info(f"**** produce job_messages {job_messages}")

            return job_messages
        except ClientError as error:
            log.error("Couldn't receive messages from queue: %s", self.report_request_queue)
            raise error
