.PHONY: up up-full down nuke topics seed init-cdc demo-slice test e2e

COMPOSE := docker compose

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

seed:
	@echo "Seeding registry with ROOT, Rucio, XRootD..."
	@curl -sf -X POST http://127.0.0.1:8080/api/v1/projects \
		-H "Content-Type: application/json" \
		-H "X-API-Key: $${REGISTRY_API_KEY:-scipulse-registry-dev-key-change-me}" \
		-d '{"slug":"root","name":"ROOT","keywords":["ROOT","C++","HEP"],"topics":["hep-ex","physics.data-an"],"priority":1}' >/dev/null || true
	@curl -sf -X POST http://127.0.0.1:8080/api/v1/projects \
		-H "Content-Type: application/json" \
		-H "X-API-Key: $${REGISTRY_API_KEY:-scipulse-registry-dev-key-change-me}" \
		-d '{"slug":"rucio","name":"Rucio","keywords":["Rucio","data management"],"topics":["cs.DC","hep-ex"],"priority":1}' >/dev/null || true
	@curl -sf -X POST http://127.0.0.1:8080/api/v1/projects \
		-H "Content-Type: application/json" \
		-H "X-API-Key: $${REGISTRY_API_KEY:-scipulse-registry-dev-key-change-me}" \
		-d '{"slug":"xrootd","name":"XRootD","keywords":["XRootD","storage"],"topics":["cs.DC","hep-ex"],"priority":2}' >/dev/null || true

demo-slice:
	$(COMPOSE) run --rm spark /opt/spark/bin/spark-submit --master "local[*]" /opt/spark/jobs/bronze_ingest.py --trigger availableNow
	$(COMPOSE) run --rm spark /opt/spark/bin/spark-submit --master "local[*]" /opt/spark/jobs/bronze_to_silver.py
	$(COMPOSE) run --rm spark /opt/spark/bin/spark-submit --master "local[*]" /opt/spark/jobs/enrich_projects.py
	$(COMPOSE) run --rm spark /opt/spark/bin/spark-submit --master "local[*]" /opt/spark/jobs/silver_to_gold_trends.py
	$(COMPOSE) run --rm spark /opt/spark/bin/spark-submit --master "local[*]" /opt/spark/jobs/silver_to_gold_impact.py

test:
	@echo "Run Go and Python unit tests when present"

e2e:
	@echo "Run offline E2E slice"
