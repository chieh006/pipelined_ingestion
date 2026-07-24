# Thin convenience wrappers. All real logic lives in docker-compose.yml,
# scripts/, and the Python package, so Windows users can run the underlying
# commands directly -- Make is a convenience, not a dependency.
TIER   ?= small
BUCKET ?= bronze
DELAY  ?= 1ms
COMPOSE = docker compose

.PHONY: help rgw-up rgw-down minio-up minio-down seed netem-set netem-clear test

help:  ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n", $$1, $$2}'

rgw-up:  ## start Ceph RGW fixture, wait for healthy
	$(COMPOSE) --profile rgw up -d --wait

rgw-down:  ## stop RGW and wipe its volumes (full reset)
	$(COMPOSE) --profile rgw down -v

minio-up:  ## start MinIO (CI / correctness only)
	$(COMPOSE) --profile minio up -d --wait

minio-down:  ## stop MinIO and wipe its volumes
	$(COMPOSE) --profile minio down -v

seed:  ## seed a corpus, e.g. make seed TIER=small BUCKET=bronze
	uv run python -m rgw_ingest_bench seed --tier $(TIER) --bucket $(BUCKET)

netem-set:  ## inject latency, e.g. make netem-set DELAY=1ms
	sudo scripts/netem.sh set $(DELAY)

netem-clear:  ## remove injected latency
	sudo scripts/netem.sh clear

test:  ## fast gate: unit + moto, 100% line/branch coverage
	uv run pytest -m "not integration and not minio and not netem" \
		--cov=rgw_ingest_bench --cov-branch --cov-fail-under=100
