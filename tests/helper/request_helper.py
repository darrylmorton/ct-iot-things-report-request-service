import datetime
import json
import logging
import time
import uuid
from typing import Any

import boto3

from things_report_request_service.service import ThingsReportRequestService
from util.service_util import create_report_request_timestamp, get_date_range_days, create_job_message

log = logging.getLogger("service")


def create_sqs_queue(queue_name: str):
    sqs = boto3.resource("sqs", region_name="eu-west-2")

    queue = sqs.create_queue(
        QueueName=f"{queue_name}.fifo",
        Attributes={'DelaySeconds': '5'}
    )
    log.info(f"**** queue {queue}")

    return queue


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


def expected_job_messages(messages: Any):
    job_messages = []
    # log.info(f"**** expected_job_messages len(messages) {len(messages)}")

    # log.info(f"**** expected_job_messages messages {messages[0]['MessageBody']}")
    # log.info(f"**** **** expected_job_messages messages {messages}")

    for message in messages:
        # log.info(f"**** **** create_job_message {message['MessageBody']}")

        message_body = json.loads(message['MessageBody'])
        # log.info(f"**** **** create_job_message message_body {message_body}")

        # log.info(f"**** **** create_job_message {message_body['StartTimestamp']}")

        start_timestamp_iso = message_body["StartTimestamp"]
        start_timestamp = create_report_request_timestamp(start_timestamp_iso)
        end_timestamp_iso = message_body["EndTimestamp"]
        end_timestamp = create_report_request_timestamp(end_timestamp_iso)

        date_range_days = get_date_range_days(start_timestamp, end_timestamp)
        log.info(f"**** **** date_range_days {date_range_days}")

        for index in range(int(date_range_days)):
            log.info(f"**** **** index {index}")

            date = create_report_request_timestamp(start_timestamp_iso)
            # log.info(f"**** **** date {date}")

            datetime_delta = datetime.timedelta(days=index)
            log.info(f"**** **** datetime_delta {datetime_delta}")

            job_start_date = date.replace(hour=0, minute=0, second=0) + datetime_delta
            job_end_date = date.replace(hour=23, minute=59, second=59) + datetime_delta

            # log.info(f"**** **** job_start_date {job_start_date}")
            # log.info(f"**** **** job_end_date {job_end_date}")

            message_id = uuid.uuid4()
            # log.info(f"**** message_id {message_id}")
            archive_report = index < int(date_range_days) - 1

            job_message = create_job_message(
                message_id=str(message_id),
                user_id=message_body["UserId"],
                report_name=message_body["ReportName"],
                start_timestamp=job_start_date.isoformat(),
                end_timestamp=job_end_date.isoformat(),
                job_index=str(index),
                total_jobs=date_range_days,
                archive_report=str(archive_report)
            )

            job_messages.append(job_message)

            # message.delete()

    return job_messages


def service_poll(request_service: ThingsReportRequestService, timeout_seconds=0):
    timeout = time.time() + timeout_seconds
    request_messages = []

    while True:
        if time.time() > timeout:
            log.info(f"Task timed out after {timeout_seconds}")
            break
        else:
            # request_messages = request_messages + request_service.consume()
            request_service.consume()
            # result = request_service.produce(request_messages)
            # log.info(f"**** service_poll result {result}")
    # log.info(f"**** SERVICE POLL request_messages {request_messages}")
    # log.info(f"**** SERVICE POLL len(request_messages) {len(request_messages)}")

    # return request_messages


def report_jobs_consumer(report_job_queue: Any, timeout_seconds=0) -> Any:
    log.info(f"**** report_jobs_consumer called...")

    timeout = time.time() + timeout_seconds
    messages = []

    while True:
        if time.time() > timeout:
            log.info(f"Task timed out after {timeout_seconds}")
            break

        job_messages = report_job_queue.receive_messages(
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )
        log.info(f"**** report_jobs_consumer job_messages {len(job_messages)}")

        for job_message in job_messages:
            messages.append(job_message)

            job_message.delete()

    return messages


def assert_request_messages(actual_result: Any, expected_result: Any):
    assert len(actual_result) == len(expected_result)
    index = 0

    for request_message in actual_result:
        assert request_message.body == expected_result[index]["MessageBody"]
        index = index + 1


def assert_job_messages(actual_result: Any, expected_result: Any):
    log.info(f"**** assert_job_messages len(actual_result): {len(actual_result)}")
    log.info(f"**** assert_job_messages len(expected_result): {len(expected_result)}")

    assert len(actual_result) == len(expected_result) + 1
    index = 0

    for job_message in actual_result:
        job_message_body = json.loads(job_message.body)

        if index < len(actual_result) - 1:
            assert job_message_body == expected_result[index]["MessageBody"]
        else:
            assert job_message_body["ArchiveReport"] == "True"

        index = index + 1
