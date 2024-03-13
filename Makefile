.DEFAULT_GOAL := build

fmt:
	poetry run ruff format .
.PHONY:fmt

lint: fmt
	poetry run ruff check . --fix
.PHONY:lint

server-start: fmt
	poetry run python -m things_report_request_service
.PHONY:server-start

test: fmt
	poetry run python -m pytest
.PHONY:test