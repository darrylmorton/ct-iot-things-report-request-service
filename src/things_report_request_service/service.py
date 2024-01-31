# Get the service resource
import logging

import boto3
from botocore.exceptions import ClientError

from config import THINGS_REPORT_REQUEST_QUEUE

log = logging.getLogger("service")


class ThingsReportRequestService:
    def __init__(self):
        self.running = True

    def stop(self):
        self.running = False

    def start(self):
        log.info("**** running...")
        sqs = boto3.resource('sqs', region_name="eu-west-2")
        # Get the queue
        queue = sqs.Queue(f"{THINGS_REPORT_REQUEST_QUEUE}.fifo")
        log.info(f"**** queue {queue}")

        while self.running:
            try:
                messages = queue.receive_messages(
                    MessageAttributeNames=["All"],
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=0,
                )
                log.info(f"**** messages {messages}")

                for msg in messages:
                    log.info("Received message: %s: %s", msg.message_id, msg.body)

                    # Let the queue know that the message is processed
                    # msg.delete()

            except ClientError as error:
                log.error("Couldn't receive messages from queue: %s", queue)
                raise error
