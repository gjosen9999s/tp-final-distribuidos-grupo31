SHELL := /bin/bash

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

split-data:
	docker compose -f docker-compose.yaml run --rm --no-deps --build -v "$$(pwd)/data:/data" article_handler python -c "import pyarrow.parquet as pq; t=pq.ParquetFile('/app/src/client/test.parquet').read(); n=t.num_rows; pq.write_table(t, '/data/input_1.parquet'); [pq.write_table(t.slice(i*n//3, (i+1)*n//3-i*n//3), f'/data/e2e_{i+1}.parquet') for i in range(3)]; print('split', n)"
.PHONY: split-data

q1-test:
	docker compose -f tests/docker-compose.q1-test.yaml up --build --abort-on-container-exit --exit-code-from simulator; \
	EXIT=$$?; docker compose -f tests/docker-compose.q1-test.yaml down -v; exit $$EXIT
.PHONY: q1-test

e2e:
	docker compose -f tests/docker-compose.e2e.yaml up --build --abort-on-container-exit --exit-code-from client; \
	EXIT=$$?; docker compose -f tests/docker-compose.e2e.yaml down -v; exit $$EXIT
.PHONY: e2e

e2e-3-1:
	docker compose -f tests/docker-compose.e2e-3-1.yaml up --build --abort-on-container-exit --exit-code-from runner; \
	EXIT=$$?; docker compose -f tests/docker-compose.e2e-3-1.yaml down -v; exit $$EXIT
.PHONY: e2e-3-1

e2e-3-2:
	docker compose -f tests/docker-compose.e2e-3-2.yaml up --build --abort-on-container-exit --exit-code-from runner; \
	EXIT=$$?; docker compose -f tests/docker-compose.e2e-3-2.yaml down -v; exit $$EXIT
.PHONY: e2e-3-2

e2e-3-3:
	docker compose -f tests/docker-compose.e2e-3-3.yaml up --build --abort-on-container-exit --exit-code-from runner; \
	EXIT=$$?; docker compose -f tests/docker-compose.e2e-3-3.yaml down -v; exit $$EXIT
.PHONY: e2e-3-3

