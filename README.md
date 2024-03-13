# ct-iot-things-report-request-service

## Description

The `things-report-request-service` consumes messages from `request-queue` and generates `report job messages`, then
places them onto the `job-queue`.

[Diagrams](./docs/DIAGRAMS.md)

## Run

```
make server-start
```

### Test

```
make test
```

## Deployment

Must be deployed **after** `things-service` and `things report job queue`.
