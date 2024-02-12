import json
import logging
import uuid

import boto3
from botocore.exceptions import ClientError

from config import THINGS_REPORT_REQUEST_QUEUE, THINGS_REPORT_JOB_QUEUE
from util.service_util import create_job_message

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
            self.consume()

    def consume(self):
        try:
            request_messages = self.report_request_queue.receive_messages(
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
            )
            # log.info(f"request_messages {request_messages}")

            total_messages = len(request_messages)
            log.info(f"**** total_messages {total_messages}")

            # if total_messages > 0:
            #     create_job_messages(request_messages, total_messages)

            job_messages = []
            total_jobs = total_messages + 1
            index = 0

            for request_message in request_messages:
                log.info(f"Received request_message: {request_message}")

                message_id = uuid.uuid4()
                message_compressor = index < total_messages - 1
                request_message_body = json.loads(request_message.body)
                log.info(f"Received request_message_body: {request_message_body}")

                job_message = create_job_message(
                    message_id=str(message_id),
                    user_id=request_message_body["UserId"],
                    report_name=request_message_body["ReportName"],
                    start_timestamp=request_message_body["StartTimestamp"],
                    end_timestamp=request_message_body["EndTimestamp"],
                    job_index=str(index),
                    total_jobs=str(total_jobs),
                    message_compressor=str(message_compressor)
                )

                if message_compressor:
                    job_message = create_job_message(
                        message_id=str(message_id),
                        user_id=request_message_body["UserId"],
                        report_name=request_message_body["ReportName"],
                        start_timestamp=request_message_body["StartTimestamp"],
                        end_timestamp=request_message_body["EndTimestamp"],
                        job_index=str(index + 1),
                        total_jobs=str(total_jobs),
                        message_compressor=str(message_compressor)
                    )

                # Let the queue know that the message is processed
                request_message.delete()

                job_messages.append(job_message)

                index = index + 1

            log.info(f"**** job_messages: {job_messages}")
            log.info(f"**** len(job_messages): {len(job_messages)}")

            if total_messages > 0:
                self.report_job_queue.send_messages(Entries=job_messages)

        except ClientError as error:
            log.error("Couldn't receive messages from queue: %s", self.report_request_queue)
            raise error
