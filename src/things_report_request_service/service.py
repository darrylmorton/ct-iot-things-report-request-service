import logging
from typing import Any

import boto3
from botocore.exceptions import ClientError

from config import THINGS_REPORT_REQUEST_QUEUE

log = logging.getLogger("things_report_request_service")


class ThingsReportRequestService:
    def __init__(self):
        self.sqs = boto3.resource("sqs", region_name="eu-west-2")
        self.queue = self.sqs.Queue(f"{THINGS_REPORT_REQUEST_QUEUE}.fifo")

        log.info(f"queue {self.queue}")

    def poll(self):
        while True:
            self.consume()

    def create_job(self, message: Any):
        return False

    def consume(self):
        try:
            messages = self.queue.receive_messages(
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
            )
            log.info(f"messages {messages}")

            for msg in messages:
                # log.info("Received message: %s: %s", msg.Id, msg.MessageBody)
                log.info(f"Received message: {msg.body}")
                # log.info(f"Received message: {msg}")
                message_body = msg.body

                # Let the queue know that the message is processed
                msg.delete()

        except ClientError as error:
            log.error("Couldn't receive messages from queue: %s", self.queue)
            raise error
