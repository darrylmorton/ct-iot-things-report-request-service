import os

from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION")

ENVIRONMENT = os.environ.get("ENVIRONMENT")
LOG_LEVEL = os.environ.get("LOG_LEVEL")
SERVICE_NAME = os.environ.get("SERVICE_NAME")

THINGS_REPORT_REQUEST_QUEUE = os.environ.get("THINGS_REPORT_REQUEST_QUEUE")
THINGS_REPORT_JOB_QUEUE = os.environ.get("THINGS_REPORT_JOB_QUEUE")
