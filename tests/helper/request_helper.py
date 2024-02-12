import datetime
import json
import logging
import time
import uuid
from typing import Any

import boto3

from things_report_request_service.service import ThingsReportRequestService

log = logging.getLogger("service")


def create_sqs_queue(queue_name: str):
    sqs = boto3.resource("sqs", region_name="eu-west-2")

    queue = sqs.create_queue(
        QueueName=f"{queue_name}.fifo",
        Attributes={'DelaySeconds': '5'}
    )
    log.info(f"**** queue {queue}")

    return queue


def create_report_request_timestamp(iso_date: str) -> datetime:
    return datetime.datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%S")


def create_timestamp(days: int = 0, before: bool = False) -> datetime:
    delta = datetime.timedelta(days=days)
    timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    log.info(f"**** create_timestamp - timestamp {timestamp}")

    if before:
        log.info(f"**** create_timestamp - timestamp {timestamp - delta}")

        return timestamp - delta
    else:
        log.info(f"**** create_timestamp - timestamp {timestamp + delta}")

        return timestamp + delta


def get_date_range_days(start: datetime, end: datetime) -> str:
    return f"{int((end - start).days)}"


def create_messages(total: int, offset=0):
    messages = []
    year_delta = 10

    for counter in range(total):
        index = counter + offset

        start_timestamp_isoformat = f"20{year_delta}-01-01T00:00:00"
        # log.info(f"**** start_timestamp_isoformat {start_timestamp_isoformat}")

        start_timestamp = create_report_request_timestamp(start_timestamp_isoformat)
        # start_time_stamp = start_time_stamp.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        # log.info(f"**** start_timestamp {start_timestamp}")
        # print(f"**** start_timestamp {start_timestamp}")

        year_delta = year_delta + 1
        end_timestamp_isoformat = f"20{year_delta}-01-01T00:00:00"
        # log.info(f"**** end_timestamp_isoformat {end_timestamp_isoformat}")

        end_timestamp = create_report_request_timestamp(end_timestamp_isoformat)
        # end_time_stamp = end_time_stamp.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        # log.info(f"**** end_timestamp {end_timestamp}")

        date_range_days = get_date_range_days(start_timestamp, end_timestamp)
        # log.info(f"**** **** date_range_days {date_range_days}")

        message_id = uuid.uuid4()
        # log.info(f"**** message_id {message_id}")

        user_id = uuid.uuid4()
        # log.info(f"**** user_id {user_id}")

        messages.append({
            "Id": str(message_id),
            "MessageAttributes": {
                "Id": {
                    "DataType": "String",
                    "StringValue": str(message_id),
                },
                "UserId": {
                    "DataType": "String",
                    "StringValue": str(user_id),
                },
                "ReportName": {
                    "DataType": "String",
                    "StringValue": f"report_name_{index}",
                },
                "StartTimestamp": {
                    "DataType": "String",
                    "StringValue": start_timestamp_isoformat,
                },
                "EndTimestamp": {
                    "DataType": "String",
                    "StringValue": end_timestamp_isoformat,
                },
                "DateRangeDays": {
                    "DataType": "String",
                    "StringValue": date_range_days
                }
            },
            "MessageBody": json.dumps({
                "Id": str(message_id),
                "UserId": str(user_id),
                "ReportName": f"report_name_{index}",
                "StartTimestamp": start_timestamp_isoformat,
                "EndTimestamp": end_timestamp_isoformat,
                "DateRangeDays": date_range_days,
            }),
            "MessageDeduplicationId": str(message_id),
        })

        year_delta = year_delta + 1

    return messages


def service_poll(request_service: ThingsReportRequestService, timeout_seconds=0):
    timeout = time.time() + timeout_seconds

    while True:
        if time.time() > timeout:
            log.info(f"Task timed out after {timeout_seconds}")
            break

        request_service.consume()


def service_consume(report_job_queue: Any, timeout_seconds=0):
    log.info(f"**** service_consume called...")

    timeout = time.time() + timeout_seconds

    while True:
        if time.time() > timeout:
            log.info(f"Task timed out after {timeout_seconds}")
            break

        job_messages = report_job_queue.receive_messages(
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )
        log.info(f"len(job_messages( {len(job_messages)}")

        # for job_messages in job_messages:
        #     message_body = json.loads(job_messages.body)
        #     # log.info(f"job_messages {job_messages}")
        #     log.info(f"job_message_body {message_body}")
