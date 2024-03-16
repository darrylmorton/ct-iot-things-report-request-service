import datetime
import json
import logging
import uuid
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ..config import (
    THINGS_REPORT_REQUEST_QUEUE,
    THINGS_REPORT_JOB_QUEUE,
    AWS_DEFAULT_REGION,
)
from ..util.service_util import (
    get_date_range_days,
    create_report_timestamp,
    create_job_message,
)

log = logging.getLogger("things_report_request_service")


class ThingsReportRequestService:
    def __init__(self):
        self.sqs = boto3.resource("sqs", region_name=AWS_DEFAULT_REGION)
        self.report_request_queue = self.sqs.Queue(
            f"{THINGS_REPORT_REQUEST_QUEUE}.fifo"
        )
        # self.report_request_dlq = self.sqs.Queue(f"{THINGS_REPORT_REQUEST_DLQ}.fifo")
        self.report_job_queue = self.sqs.Queue(f"{THINGS_REPORT_JOB_QUEUE}.fifo")

    def poll(self):
        while True:
            self.consume()

    def consume(self):
        try:
            request_messages = self.report_request_queue.receive_messages(
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=10,
                # WaitTimeSeconds=WAIT_SECONDS,
            )

            if len(request_messages) > 0:
                job_messages = []

                for request_message in request_messages:
                    message_body = json.loads(request_message.body)
                    request_message.delete()

                    start_timestamp_iso = message_body["StartTimestamp"]
                    start_timestamp = create_report_timestamp(start_timestamp_iso)
                    end_timestamp_iso = message_body["EndTimestamp"]
                    end_timestamp = create_report_timestamp(end_timestamp_iso)

                    date_range_days = get_date_range_days(
                        start_timestamp, end_timestamp
                    )
                    total_jobs = date_range_days + 1

                    date_range_days_countdown = total_jobs
                    counter = 1

                    for index in range(total_jobs):
                        date = create_report_timestamp(start_timestamp_iso)

                        datetime_delta = datetime.timedelta(days=index)

                        job_start_date = (
                            date.replace(hour=0, minute=0, second=0) + datetime_delta
                        )
                        job_end_date = (
                            date.replace(hour=23, minute=59, second=59) + datetime_delta
                        )

                        message_id = uuid.uuid4()

                        archive_report = index > date_range_days - 1

                        job_message = create_job_message(
                            message_id=str(message_id),
                            user_id=message_body["UserId"],
                            report_name=message_body["ReportName"],
                            start_timestamp=job_start_date.isoformat(),
                            end_timestamp=job_end_date.isoformat(),
                            job_index=str(index),
                            total_jobs=str(total_jobs),
                            archive_report=str(archive_report),
                        )

                        job_messages.append(job_message)

                        if date_range_days_countdown <= 10 or counter == 10:
                            self.produce(job_messages)

                            job_messages = []
                            counter = 1

                        counter = counter + 1
                        date_range_days_countdown = date_range_days_countdown - 1

        except ClientError as error:
            log.error(
                f"Couldn't receive report_request_queue messages client error {error}"
            )
            raise error

    def produce(self, job_messages: Any) -> Any:
        try:
            if len(job_messages) > 0:
                log.info(f"produce - job_messages {job_messages}")

                self.report_job_queue.send_messages(Entries=job_messages)

            return job_messages
        except ClientError as error:
            log.error(f"Couldn't receive report_job_queue messages error {error}")

            raise error
