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


def create_messages(total: int, offset=0):
    messages = []
    # index = total + offset

    for counter in range(total):
        index = counter + offset

        message_id = uuid.uuid4(),
        log.info(f"**** message_id {message_id[0]}")

        messages.append({
            "Id": str(message_id[0]),
            "MessageAttributes": {
                "Thing": {
                    "DataType": "String",
                    "StringValue": f"Thing{index}",
                },
                "ThingType": {
                    "DataType": "String",
                    "StringValue": f"ThingType{index}",
                },
                "StartDate": {
                    "DataType": "String",
                    "StringValue": f"Day{index}",
                },
                "EndDate": {
                    "DataType": "String",
                    "StringValue": f"Day{index}",
                }
            },
            "MessageBody": json.dumps({
                "Thing": f"Thing{index}",
                "ThingType": f"ThingType{index}",
                "StartDate": f"Day{index}",
                "EndDate": f"Day{index}",
            }),
            "MessageDeduplicationId": str(message_id[0]),
        })

        # index += 1

    return messages


def service_poll(request_service: ThingsReportRequestService, timeout_seconds=0):
    timeout = time.time() + timeout_seconds

    while True:
        if time.time() > timeout:
            log.info(f"Task timed out after {timeout_seconds}")
            break

        request_service.consume()
