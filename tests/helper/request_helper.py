import datetime
import json
import logging
import time
import uuid
from typing import Any

import boto3

from src.config import AWS_DEFAULT_REGION
from src.things_report_request_service.service import ThingsReportRequestService
from src.util.service_util import (
    create_report_timestamp,
    get_date_range_days,
    create_job_message,
)

log = logging.getLogger("test_things_report_request_service")


def create_sqs_queue(queue_name: str, dlq_name=""):
    log.info(f"create_sqs_queue - dlq_name {dlq_name}")

    sqs = boto3.resource("sqs", region_name=AWS_DEFAULT_REGION)
    queue_attributes = {
        "DelaySeconds": "5",
    }
    dlq = None

    if dlq_name:
        dlq = sqs.create_queue(
            QueueName=f"{dlq_name}.fifo", Attributes=queue_attributes
        )

        dlq_policy = json.dumps({
            "deadLetterTargetArn": dlq.attributes["QueueArn"],
            "maxReceiveCount": "10",
        })

        queue_attributes["RedrivePolicy"] = dlq_policy

    queue = sqs.create_queue(
        QueueName=f"{queue_name}.fifo", Attributes=queue_attributes
    )

    return queue, dlq


def create_timestamp(days: int = 0, before: bool = False) -> datetime:
    delta = datetime.timedelta(days=days)
    timestamp = datetime.datetime.now(tz=datetime.timezone.utc)

    if before:
        return timestamp - delta
    else:
        return timestamp + delta


def report_request_dlq_consumer(report_request_dlq: Any, timeout_seconds=0) -> Any:
    timeout = time.time() + timeout_seconds
    messages = []

    while True:
        if time.time() > timeout:
            log.info(f"Task timed out after {timeout_seconds}")
            break

        log.info(f"report_request_dlq: {report_request_dlq}")

        messages = report_request_dlq.receive_messages(
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )

        for message in messages:
            messages.append(message)

            message.delete()

    return messages


def create_request_message(
        message_id: str,
        user_id: str,
        report_name: str,
        start_timestamp: str,
        end_timestamp: str,
        date_range_days: str,
):
    return {
        "Id": message_id,
        "MessageAttributes": {
            "Id": {
                "DataType": "String",
                "StringValue": message_id,
            },
            "UserId": {
                "DataType": "String",
                "StringValue": user_id,
            },
            "ReportName": {
                "DataType": "String",
                "StringValue": report_name,
            },
            "StartTimestamp": {
                "DataType": "String",
                "StringValue": start_timestamp,
            },
            "EndTimestamp": {
                "DataType": "String",
                "StringValue": end_timestamp,
            },
            "DateRangeDays": {
                "DataType": "String",
                "StringValue": date_range_days,
            },
        },
        "MessageBody": json.dumps({
            "Id": message_id,
            "UserId": user_id,
            "ReportName": report_name,
            "StartTimestamp": start_timestamp,
            "EndTimestamp": end_timestamp,
            "DateRangeDays": date_range_days,
        }),
        "MessageDeduplicationId": message_id,
    }


def create_request_messages(total: int, offset=0):
    messages = []
    year_delta = 10

    for counter in range(total):
        index = counter + offset

        start_timestamp_isoformat = f"20{year_delta}-01-01T00:00:00"
        start_timestamp = create_report_timestamp(start_timestamp_isoformat)

        year_delta = year_delta + 1
        end_timestamp_isoformat = f"20{year_delta}-01-01T00:00:00"

        end_timestamp = create_report_timestamp(end_timestamp_isoformat)

        date_range_days = get_date_range_days(start_timestamp, end_timestamp)

        message_id = uuid.uuid4()
        user_id = uuid.uuid4()

        request_message = create_request_message(
            message_id=str(message_id),
            user_id=str(user_id),
            report_name=f"report_name_{index}",
            start_timestamp=start_timestamp_isoformat,
            end_timestamp=end_timestamp_isoformat,
            date_range_days=str(date_range_days),
        )
        messages.append(request_message)

        year_delta = year_delta + 1

    return messages


def expected_job_messages(messages: Any):
    job_messages = []

    for message in messages:
        message_body = json.loads(message["MessageBody"])

        start_timestamp_iso = message_body["StartTimestamp"]
        start_timestamp = create_report_timestamp(start_timestamp_iso)
        end_timestamp_iso = message_body["EndTimestamp"]
        end_timestamp = create_report_timestamp(end_timestamp_iso)

        date_range_days = get_date_range_days(start_timestamp, end_timestamp)
        total_jobs = date_range_days + 1

        for index in range(total_jobs):
            date = create_report_timestamp(start_timestamp_iso)

            datetime_delta = datetime.timedelta(days=index)

            job_start_date = date.replace(hour=0, minute=0, second=0) + datetime_delta
            job_end_date = date.replace(hour=23, minute=59, second=59) + datetime_delta

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

    return job_messages


def service_poll(request_service: ThingsReportRequestService, timeout_seconds=0):
    timeout = time.time() + timeout_seconds

    while True:
        if time.time() > timeout:
            log.info(f"Task timed out after {timeout_seconds}")
            break
        else:
            request_service.consume()


def report_jobs_consumer(report_job_queue: Any, timeout_seconds=0) -> Any:
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

        for job_message in job_messages:
            messages.append(job_message)

            job_message.delete()

    return messages


def validate_uuid4(uuid_string):
    """
    Validate that a UUID string is in
    fact a valid uuid4.
    Happily, the uuid module does the actual
    checking for us.
    It is vital that the 'version' kwarg be passed
    to the UUID() call, otherwise any 32-character
    hex string is considered valid.
    """

    try:
        val = uuid.UUID(uuid_string, version=4)

    except ValueError:
        # If it's a value error, then the string
        # is not a valid hex code for a UUID.
        return False

    # If the uuid_string is a valid hex code,
    # but an invalid uuid4,
    # the UUID.__init__ will convert it to a
    # valid uuid4. This is bad for validation purposes.

    return str(val) == uuid_string


def assert_request_messages(actual_result: Any, expected_result: Any):
    assert len(actual_result) == len(expected_result)
    index = 0

    for request_message in actual_result:
        assert request_message.body == expected_result[index]["MessageBody"]
        index = index + 1


def assert_job_message(actual_result: Any, expected_result: Any):
    assert validate_uuid4(actual_result["Id"])
    assert validate_uuid4(expected_result["Id"])
    assert actual_result["Id"] != expected_result["Id"]

    assert actual_result["UserId"] == expected_result["UserId"]
    assert actual_result["ReportName"] == expected_result["ReportName"]
    assert actual_result["StartTimestamp"] == expected_result["StartTimestamp"]
    assert actual_result["EndTimestamp"] == expected_result["EndTimestamp"]
    assert actual_result["JobIndex"] == expected_result["JobIndex"]
    assert actual_result["TotalJobs"] == expected_result["TotalJobs"]
    assert actual_result["ArchiveReport"] == expected_result["ArchiveReport"]


def assert_job_messages(actual_result: Any, expected_result: Any):
    # log.info(f"actual_result: {actual_result}")
    log.info(f"actual_result length:{len(actual_result)}")

    # log.info(f"expected_result: {expected_result}")
    log.info(f"expected_result length:{len(expected_result)}")

    # assert len(actual_result) == len(expected_result)
    index = 0

    for job_message in actual_result:
        job_message_body = json.loads(job_message.body)
        log.info(f"actual_result_body: {job_message_body}")

        expected_message = expected_result[index]
        expected_result_body = json.loads(expected_message["MessageBody"])
        log.info(f"expected_result_body: {expected_result_body}")

        assert_job_message(job_message_body, expected_result_body)

        index = index + 1
