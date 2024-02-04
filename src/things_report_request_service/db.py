import boto3

DB_NAME = "thing_report_requests"

dynamodb = boto3.resource("dynamodb", region_name="eu-west-2")

table = dynamodb.create_table(
    TableName=DB_NAME,
    AttributeDefinitions=[
        {
            "AttributeName": "id",
            "AttributeType": "S",
        },
        {
            "AttributeName": "userId",
            "AttributeType": "S",
        },
        {
            "AttributeName": "reportName",
            "AttributeType": "S",
        },
        {
            "AttributeName": "startTimestamp",
            "AttributeType": "S",
        },
        {
            "AttributeName": "endTimestamp",
            "AttributeType": "S",
        },
        {
            "AttributeName": "updatedAt",
            "AttributeType": "S",
        },
    ],
    KeySchema=[
        {
            "AttributeName": "id",
            "KeyType": "HASH",
        },
    ],
    ProvisionedThroughput={
        "ReadCapacityUnits": 1,
        "WriteCapacityUnits": 1,
    },
    StreamSpecification={
        "StreamEnabled": False,
    },
    BillingMode="PAY_PER_REQUEST",
    GlobalSecondaryIndexes=[
        {
            "IndexName": "userIdIndex",
            "KeySchema": [
                {
                    "AttributeName": "userId",
                    "KeyType": "HASH",
                },
                {
                    "AttributeName": "updatedAt",
                    "KeyType": "RANGE",
                },
            ],
            "Projection": {
                "ProjectionType": "ALL",
            },
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        },
        {
            "IndexName": "reportNameIndex",
            "KeySchema": [
                {
                    "AttributeName": "reportName",
                    "KeyType": "HASH",
                },
                {
                    "AttributeName": "updatedAt",
                    "KeyType": "RANGE",
                },
            ],
            "Projection": {
                "ProjectionType": "ALL",
            },
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        },
        {
            "IndexName": "startTimestampIndex",
            "KeySchema": [
                {
                    "AttributeName": "startTimestamp",
                    "KeyType": "HASH",
                },
                {
                    "AttributeName": "updatedAt",
                    "KeyType": "RANGE",
                },
            ],
            "Projection": {
                "ProjectionType": "ALL",
            },
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        },
        {
            "IndexName": "endTimestampIndex",
            "KeySchema": [
                {
                    "AttributeName": "endTimestamp",
                    "KeyType": "HASH",
                },
                {
                    "AttributeName": "updatedAt",
                    "KeyType": "RANGE",
                },
            ],
            "Projection": {
                "ProjectionType": "ALL",
            },
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        },
    ],
)

# Wait until the table exists.
table.wait_until_exists()

# Print out some data about the table.
print(table.item_count)
