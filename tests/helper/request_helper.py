import json
import logging
import uuid

log = logging.getLogger("service")


def create_messages(total: int, offset=0):
    messages = []
    index = offset

    for counter in range(total - offset):
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

# def send_messages(queue, messages):
#     """
#     Send a batch of messages in a single request to an SQS queue.
#     This request may return overall success even when some messages were not sent.
#     The caller must inspect the Successful and Failed lists in the response and
#     resend any failed messages.
#
#     :param queue: The queue to receive the messages.
#     :param messages: The messages to send to the queue. These are simplified to
#                      contain only the message body and attributes.
#     :return: The response from SQS that contains the list of successful and failed
#              messages.
#     """
#     try:
#         entries = [
#             {
#                 "Id": str(ind),
#                 "MessageBody": msg["body"],
#                 "MessageAttributes": msg["attributes"],
#             }
#             for ind, msg in enumerate(messages)
#         ]
#         response = queue.send_messages(Entries=entries)
#         if "Successful" in response:
#             for msg_meta in response["Successful"]:
#                 logging.info(
#                     "Message sent: %s: %s",
#                     msg_meta["MessageId"],
#                     messages[int(msg_meta["Id"])]["body"],
#                 )
#         if "Failed" in response:
#             for msg_meta in response["Failed"]:
#                 logging.warning(
#                     "Failed to send: %s: %s",
#                     msg_meta["MessageId"],
#                     messages[int(msg_meta["Id"])]["body"],
#                 )
#     except ClientError as error:
#         logging.exception("Send messages failed to queue: %s", queue)
#         raise error
#     else:
#         return response
#
#
# send_messages(request_queue, messages)

# Print out any failures
# print(response.get("Failed"))
