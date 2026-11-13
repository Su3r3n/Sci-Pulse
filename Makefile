.PHONY: up up-full down nuke topics seed init-cdc secrets demo-slice test e2e

COMPOSE := docker compose
PYTHON  ?= python3

# Переменные из .env (если файл есть): нужны для генерации секретов и seed.
-include .env
export

# B3: Debezium читает пароль через FileConfigProvider из /secrets/debezium.properties.
# Файл генерируется из .env и монтируется в connect read-only; в git не попадает (.gitignore).
SECRETS_FILE := secrets/debezium.properties

$(SECRETS_FILE): $(wildcard .env)
	@mkdir -p secrets
	@umask 077 && printf 'user=%s\npassword=%s\n' \
		"$${DEBEZIUM_DB_USER:-debezium}" "$${DEBEZIUM_DB_PASSWORD:-debezium_dev_pass}" > $@
	@echo "Generated $@ (from .env)"

secrets: $(SECRETS_FILE)

up: secrets
	$(COMPOSE) --profile core up -d --wait

up-full: secrets
	$(COMPOSE) --profile core --profile full up -d --wait

down:
	$(COMPOSE) --profile core --profile full down

nuke:
	$(COMPOSE) --profile core --profile full down -v --remove-orphans

topics:
	$(COMPOSE) --profile core run --rm kafka-init

init-cdc: secrets
	$(COMPOSE) --profile core run --rm connect-init

# M9: проекты ROOT/Rucio/XRootD и их primary-репозитории по ТЗ §4.3 (идемпотентно).
seed:
	$(PYTHON) reference/seed_registry.py

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
	@for m in services/common-go services/registry-go services/harvester-go; do \
		echo "go test $$m"; (cd $$m && go test ./...) || exit 1; \
	done
	$(PYTHON) -m pytest -q platform/spark

# Офлайн сквозной срез: harvester --once --offline, затем demo-slice.
e2e: secrets
	$(COMPOSE) --profile core run --rm harvester --once --offline
	$(MAKE) demo-slice
