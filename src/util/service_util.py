import datetime
import json
import logging

log = logging.getLogger("service")


def create_report_request_timestamp(iso_date: str) -> datetime:
    return datetime.datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%S")


def get_date_range_days(start: datetime, end: datetime) -> str:
    return f"{int((end - start).days)}"


def create_job_message(
        message_id: str,
        user_id: str,
        report_name: str,
        start_timestamp: str,
        end_timestamp: str,
        job_index: str,
        total_jobs: str,
        archive_report: str
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
            "JobIndex": {
                "DataType": "String",
                "StringValue": job_index,
            },
            "TotalJobs": {
                "DataType": "String",
                "StringValue": total_jobs,
            },
            "ArchiveReport": {
                "DataType": "String",
                "StringValue": archive_report,
            },
        },
        "MessageBody": json.dumps({
            "Id": message_id,
            "UserId": user_id,
            "ReportName": report_name,
            "StartTimestamp": start_timestamp,
            "EndTimestamp": end_timestamp,
            "JobIndex": job_index,
            "TotalJobs": total_jobs,
            "ArchiveReport": archive_report,
        }),
        "MessageDeduplicationId": message_id,
    }

# def create_job_messages(messages: Any):
#     job_messages = []
#     # log.info(f"**** create_job_messages len(messages) {len(messages)}")
#
#     # log.info(f"**** create_job_messages messages {messages[0]['MessageBody']}")
#     log.info(f"**** **** create_job_messages messages {messages}")
#
#     for message in messages:
#         log.info(f"**** **** create_job_message {message.body}")
#
#         message_body = json.loads(message.body)
#         log.info(f"**** **** create_job_message message_body {message_body}")
#
#         log.info(f"**** **** create_job_message {message_body['StartTimestamp']}")
#
#         start_timestamp_iso = message_body["StartTimestamp"]
#         start_timestamp = create_report_request_timestamp(start_timestamp_iso)
#         end_timestamp_iso = message_body["EndTimestamp"]
#         end_timestamp = create_report_request_timestamp(end_timestamp_iso)
#
#         date_range_days = get_date_range_days(start_timestamp, end_timestamp)
#         log.info(f"**** **** date_range_days {date_range_days}")
#
#         for index in range(int(date_range_days)):
#             log.info(f"**** **** index {index}")
#
#             date = create_report_request_timestamp(start_timestamp_iso)
#             log.info(f"**** **** date {date}")
#
#             datetime_delta = datetime.timedelta(days=index)
#             log.info(f"**** **** datetime_delta {datetime_delta}")
#
#             job_start_date = date.replace(hour=0, minute=0, second=0) + datetime_delta
#             job_end_date = date.replace(hour=23, minute=59, second=59) + datetime_delta
#
#             log.info(f"**** **** job_start_date {job_start_date}")
#             log.info(f"**** **** job_end_date {job_end_date}")
#
#             message_id = uuid.uuid4()
#             # log.info(f"**** message_id {message_id}")
#             archive_report = index < int(date_range_days) - 1
#
#             job_message = create_job_message(
#                 message_id=str(message_id),
#                 user_id=message_body["UserId"],
#                 report_name=message_body["ReportName"],
#                 start_timestamp=job_start_date.isoformat(),
#                 end_timestamp=job_end_date.isoformat(),
#                 job_index=str(index),
#                 total_jobs=date_range_days,
#                 archive_report=str(archive_report)
#             )
#
#             job_messages.append(job_message)
#
#             message.delete()
#
#     return job_messages
