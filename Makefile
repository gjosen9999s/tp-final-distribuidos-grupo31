SHELL := /bin/bash

up:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

debug:
	LOG_LEVEL=DEBUG COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: debug

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

clean:
	rm -rf data/client_1/output
	rm -rf data/client_2/output
	rm -rf data/client_3/output
	rm -rf data/client_4/output

	mkdir -p data/client_1/output/img
	mkdir -p data/client_2/output/img
	mkdir -p data/client_3/output/img
	mkdir -p data/client_4/output/img

	rm -rf output
.PHONY: clean
