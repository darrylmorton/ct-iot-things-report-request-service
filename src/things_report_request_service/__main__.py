import logging

from things_report_request_service.service import ThingsReportRequestService

log = logging.getLogger("things_report_request_service")


def main() -> None:
    log.info("Starting service")

    service = ThingsReportRequestService()
    service.poll()


if __name__ == "__main__":
    main()
