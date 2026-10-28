SHELL := /bin/bash
PWD := $(shell pwd)

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

q1-test:
	docker compose -f tests/docker-compose.q1-test.yaml up --build --abort-on-container-exit --exit-code-from simulator; \
	EXIT=$$?; docker compose -f tests/docker-compose.q1-test.yaml down -v; exit $$EXIT
.PHONY: q1-test

e2e:
	docker compose -f tests/docker-compose.e2e.yaml up --build --abort-on-container-exit --exit-code-from client; \
	EXIT=$$?; docker compose -f tests/docker-compose.e2e.yaml down -v; exit $$EXIT
.PHONY: e2e
