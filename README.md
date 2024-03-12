# ct-iot-things-report-request-service

## Description
The `things-report-request-service` consumes messages from `request-queue` and generates `report job messages`, then places them onto the `job-queue`.

[Diagrams](./docs/DIAGRAMS.md)

## Run
```
poetry run python -m things_report_request_service
```

### Test
```
poetry run python -m pytest
```

## Deployment
Must be deployed **after** `things-service` and `things report job queue`.
