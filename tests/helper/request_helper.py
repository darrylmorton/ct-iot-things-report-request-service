import datetime
import json
import logging
import time
import uuid

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
    return datetime.datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%S").timestamp()


def create_timestamp(days: int = 0, before: bool = False) -> datetime:
    delta = datetime.timedelta(days=days)
    timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    log.info(f"**** create_timestamp - timestamp {timestamp}")
    # log.info(f"**** create_timestamp - timestamp {timestamp}")

    if before:
        log.info(f"**** create_timestamp - timestamp {timestamp - delta}")

        return timestamp - delta
    else:
        log.info(f"**** create_timestamp - timestamp {timestamp + delta}")

        return timestamp + delta


def create_messages(total: int, offset=0):
    messages = []
    year_delta = 10

    for counter in range(total):
        index = counter + offset

        start_time_stamp = create_report_request_timestamp(f"20{year_delta}-01-01T00:00:00")
        log.info(f"**** start_time_stamp {start_time_stamp}")

        year_delta = year_delta + 1
        end_time_stamp = create_report_request_timestamp(f"20{year_delta}-01-01T00:00:00")
        log.info(f"**** end_time_stamp {end_time_stamp}")

        message_id = uuid.uuid4()
        log.info(f"**** message_id {message_id}")

        user_id = uuid.uuid4()
        log.info(f"**** user_id {user_id}")

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
                "StartTimeStamp": {
                    "DataType": "String",
                    "StringValue": start_time_stamp,
                },
                "EndTimeStamp": {
                    "DataType": "String",
                    "StringValue": end_time_stamp,
                }
            },
            "MessageBody": json.dumps({
                "Id": str(message_id),
                "UserId": str(user_id),
                "ReportName": f"report_name_{index}",
                "StartTimeStamp": start_time_stamp,
                "EndTimeStamp": end_time_stamp,
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
