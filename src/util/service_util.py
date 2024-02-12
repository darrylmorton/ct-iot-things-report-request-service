import json
import logging

log = logging.getLogger("service")


def create_job_message(
        message_id: str,
        user_id: str,
        report_name: str,
        start_timestamp: str,
        end_timestamp: str,
        job_index: str,
        total_jobs: str,
        message_compressor: str
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
            "CompressMessages": {
                "DataType": "String",
                "StringValue": message_compressor,
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
            "CompressMessages": message_compressor,
        }),
        "MessageDeduplicationId": message_id,
    }

# def create_job_messages(request_messages: Any, total_messages: int):
#     job_messages = []
#     total_jobs = total_messages + 1
#
#     for index, request_message in request_messages:
#         log.info(f"Received request_message: {request_message.body}")
#
#         message_id = uuid.uuid4()
#         message_compressor = index < total_messages - 1
#
#         job_message = create_job_message(
#             message_id=str(message_id),
#             user_id=request_message.UserId,
#             report_name=request_message.ReportName,
#             start_timestamp=request_message.StartTimestamp,
#             end_timestamp=request_message.EndTimestamp,
#             job_index=str(index),
#             total_jobs=str(total_jobs),
#             message_compressor=str(message_compressor)
#         )
#
#         if message_compressor:
#             job_message = create_job_message(
#                 message_id=str(message_id),
#                 user_id=request_message.UserId,
#                 report_name=request_message.ReportName,
#                 start_timestamp=request_message.StartTimestamp,
#                 end_timestamp=request_message.EndTimestamp,
#                 job_index=str(index + 1),
#                 total_jobs=str(total_jobs),
#                 message_compressor=str(message_compressor)
#             )
#
#         # Let the queue know that the message is processed
#         request_message.delete()
#
#         job_messages.append(job_message)
