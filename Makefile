.PHONY: up up-full down nuke topics seed init-cdc demo-slice test e2e

COMPOSE := docker compose
PYTHON  ?= python3

# .env читает сам docker compose (подстановка переменных в docker-compose.yml). Make не экспортирует
# весь файл в окружение рецептов (R7); точечно берём только то, что нужно seed.
-include .env
SEED_URL         ?= http://127.0.0.1:8080
REGISTRY_API_KEY ?= scipulse-registry-dev-key-change-me

# B3/R1: /secrets/debezium.properties пишет сам контейнер connect (entrypoint) из DEBEZIUM_DB_*.
up:
	$(COMPOSE) --profile core up -d --wait

up-full:
	$(COMPOSE) --profile core --profile full up -d --wait

down:
	$(COMPOSE) --profile core --profile full down

nuke:
	$(COMPOSE) --profile core --profile full down -v --remove-orphans

topics:
	$(COMPOSE) --profile core run --rm kafka-init

init-cdc:
	$(COMPOSE) --profile core run --rm connect-init

# M9: проекты ROOT/Rucio/XRootD и их primary-репозитории по ТЗ §4.3 (идемпотентно).
seed:
	SEED_URL='$(SEED_URL)' REGISTRY_API_KEY='$(REGISTRY_API_KEY)' $(PYTHON) reference/seed_registry.py

# M6: spark-задания требуют профиль core (зависимости pg-init, minio-init, ch-init).
DEMO_RUN := $(COMPOSE) --profile core --profile spark run --rm spark /opt/spark/bin/spark-submit --master "local[*]"

demo-slice:
	$(DEMO_RUN) /opt/spark/jobs/bronze_ingest.py --trigger availableNow
	$(DEMO_RUN) /opt/spark/jobs/bronze_to_silver.py
	$(DEMO_RUN) /opt/spark/jobs/enrich_projects.py
	$(DEMO_RUN) /opt/spark/jobs/silver_to_gold_trends.py
	$(DEMO_RUN) /opt/spark/jobs/silver_to_gold_impact.py

# m13: Go-модули тестируются по одному (корневой go.work не содержит tests/),
# Python-юниты платформы запускаются из platform/spark.
test:
	@for m in services/common-go services/registry-go services/harvester-go services/loadgen-go; do \
		echo "go test $$m"; (cd $$m && go test ./...) || exit 1; \
	done
	$(PYTHON) -m pytest -q platform/spark tests

# Офлайн сквозной срез: harvester --once --offline, затем demo-slice.
e2e:
	$(COMPOSE) --profile core run --rm harvester --once --offline
	$(MAKE) demo-slice
