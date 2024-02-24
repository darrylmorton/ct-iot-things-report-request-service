import datetime
import json
import logging

log = logging.getLogger("things_report_request_service")


def create_report_timestamp(iso_date: str) -> datetime:
    return datetime.datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%S")


def get_date_range_days(start: datetime, end: datetime) -> int:
    return int((end - start).days)


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
