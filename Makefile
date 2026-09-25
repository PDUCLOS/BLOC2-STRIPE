# Stripe Polyglot — Makefile
# Cœur de l'orchestration pour la démo

PROJECT_ROOT := $(shell pwd)
COMPOSE := docker compose --env-file .env
PYTHON := ./.venv/bin/python
PIP := ./.venv/bin/pip
# python3.11 si dispo (versions de requirements.txt testées dessus), sinon
# python3 du PATH — jamais un chemin absolu, pour rester portable entre
# macOS (Homebrew), Linux et les runners CI (cf. .github/workflows/ci.yml).
PYTHON_BIN := $(shell command -v python3.11 2>/dev/null || command -v python3)

# Charge .env pour les scripts qui en ont besoin
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# ─────────────────────────────────────────────────────────
# Bootstrap
# ─────────────────────────────────────────────────────────
.PHONY: init-env
init-env:
	@bash scripts/init_env.sh
	@echo ""
	@echo "Pense à remplir SNOWFLAKE_* dans .env quand tu auras créé ton compte trial"

.PHONY: venv
venv:
	@if [ ! -d ".venv" ]; then \
		$(PYTHON_BIN) -m venv .venv; \
		echo "[OK] .venv créé ($(PYTHON_BIN))"; \
	else \
		echo "[OK] .venv existe déjà"; \
	fi
	@$(PIP) install --upgrade pip

.PHONY: install
install: venv
	@$(PIP) install -r requirements.txt
	@echo "[OK] Dépendances Python installées dans .venv/"

.PHONY: init
init: init-env install
	@echo "Démarrage de l'infra..."
	@$(COMPOSE) up -d --build
	@echo ""
	@echo "Attente que tous les services soient healthy..."
	@$(COMPOSE) ps
	@echo ""
	@echo "Initialisation des topics Kafka et connecteur Debezium..."
	@sleep 10
	@bash scripts/create_topics.sh
	@bash scripts/postgres_init_roles.sh
	@bash scripts/deploy_debezium.sh
	@echo ""
	@echo "[OK] Stack prête !"

# ─────────────────────────────────────────────────────────
# Lifecycle
# ─────────────────────────────────────────────────────────
.PHONY: up
up:
	@$(COMPOSE) up -d
	@echo "[OK] Stack démarrée. Status :"
	@$(COMPOSE) ps

.PHONY: down
down:
	@$(COMPOSE) down
	@echo "[OK] Stack arrêtée (volumes conservés)"

.PHONY: clean
clean:
	@$(COMPOSE) down -v
	@echo "[OK] Stack arrêtée + volumes supprimés"
	@echo "[WARN] Tu devras relancer 'make init' pour tout reconstruire"

.PHONY: restart
restart: down up

.PHONY: logs
logs:
	@$(COMPOSE) logs -f --tail=100

.PHONY: status
status:
	@echo "Status des conteneurs :"
	@$(COMPOSE) ps
	@echo ""
	@echo "Topics Kafka :"
	@docker exec stripe-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep stripe || echo "(aucun)"
	@echo ""
	@echo "Connecteurs Debezium :"
	@curl -fsS $(KAFKA_CONNECT_URL)/connectors 2>/dev/null || echo "(Debezium pas prêt)"

# ─────────────────────────────────────────────────────────
# Données
# ─────────────────────────────────────────────────────────
.PHONY: seed
seed: mongo-indexes
	@$(PYTHON) seed/seed_data.py
	@echo "[OK] Seed terminé"

# Rejoue init/mongo/01_init_collections.js (idempotent) : les scripts de
# docker-entrypoint-initdb.d ne tournent qu'à la création du volume, donc un
# nouvel index (ex. TTL de `logs`) n'arrive jamais sur une installation existante.
.PHONY: mongo-indexes
mongo-indexes:
	@docker exec -i stripe-mongo mongosh --quiet -u "$$MONGO_USER" -p "$$MONGO_PASSWORD" \
		--authenticationDatabase admin < init/mongo/01_init_collections.js
	@echo "[OK] Collections et index MongoDB à jour"

.PHONY: producer
producer:
	@$(PYTHON) producers/transaction_producer.py

# ─────────────────────────────────────────────────────────
# Flink
# ─────────────────────────────────────────────────────────
.PHONY: flink-build
flink-build:
	@$(COMPOSE) build flink-jobmanager flink-taskmanager
	@echo "[OK] Image Flink (re)buildée"

.PHONY: flink-submit
flink-submit:
	@echo "Soumission du job PyFlink..."
	@docker exec stripe-flink-jobmanager flink run \
		--python /opt/flink/jobs/fraud_scoring_job.py \
		--jobmanager flink-jobmanager:8081
	@echo "[OK] Job soumis. Status :"
	@curl -fsS $(FLINK_JOBMANAGER_URL)/jobs 2>/dev/null | python3 -m json.tool | head -30

.PHONY: flink
flink: flink-submit
	@echo ""
	@echo "Flink UI : http://localhost:8081"

# ─────────────────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────────────────
.PHONY: dashboard
dashboard:
	@$(PYTHON) -m streamlit run dashboard/app.py \
		--server.port 8501 \
		--server.address 0.0.0.0 \
		--server.headless true \
		--browser.gatherUsageStats false

# ─────────────────────────────────────────────────────────
# Notebook d'audit données/ML
# ─────────────────────────────────────────────────────────
.PHONY: notebook notebook-check
notebook:
	@./.venv/bin/jupyter lab notebooks/audit_data_ml.ipynb

# Relance le notebook de bout en bout et échoue (exit != 0) si un des
# `assert` de contrôle échoue — utilisable en CI ou juste avant une démo.
notebook-check:
	@./.venv/bin/jupyter nbconvert --to notebook --execute --inplace \
		--ExecutePreprocessor.timeout=180 \
		--ExecutePreprocessor.kernel_name=stripe-polyglot \
		notebooks/audit_data_ml.ipynb

# ─────────────────────────────────────────────────────────
# Snowflake
# ─────────────────────────────────────────────────────────
.PHONY: snowflake-setup
snowflake-setup:
	@$(PYTHON) etl/snowflake_setup.py
	@echo "[OK] Schéma Snowflake créé (si credentials OK)"

.PHONY: snowflake-export
snowflake-export:
	@$(PYTHON) etl/load_snowflake.py
	@echo "[OK] Export batch vers Snowflake terminé"

.PHONY: refresh-views
refresh-views:
	@$(PYTHON) etl/refresh_views.py

# Livrable 8 : exécute toutes les requêtes de queries/ sur la stack qui tourne.
# ON_ERROR_STOP=1 (psql) et mongosh --file s'arrêtent au premier échec : une
# requête désalignée du schéma réel fait échouer la cible (et la CI).
# queries/snowflake_olap.sql n'est pas exécuté (Snowflake en dry-run).
.PHONY: queries-check
queries-check:
	@echo "── PostgreSQL : refresh des vues matérialisées puis queries/postgres_oltp.sql ──"
	@docker exec stripe-postgres psql -q -U $(PG_USER) -d $(PG_DB) \
		-c "REFRESH MATERIALIZED VIEW mv_daily_revenue; REFRESH MATERIALIZED VIEW mv_merchant_stats;"
	@docker exec -i stripe-postgres psql -v ON_ERROR_STOP=1 -U $(PG_USER) -d $(PG_DB) < queries/postgres_oltp.sql
	@echo "── MongoDB : queries/mongodb_queries.js (utilisateur applicatif, lecture seule) ──"
	@docker exec -i stripe-mongo sh -c 'cat > /tmp/mongodb_queries.js' < queries/mongodb_queries.js
	@docker exec stripe-mongo mongosh --quiet \
		-u "$(MONGO_APP_USER)" -p "$(MONGO_APP_PASSWORD)" --authenticationDatabase $(MONGO_DB) \
		--file /tmp/mongodb_queries.js
	@echo "[OK] Toutes les requêtes SQL et NoSQL s'exécutent sur la stack"

# ─────────────────────────────────────────────────────────
# Machine Learning
# ─────────────────────────────────────────────────────────
.PHONY: ml-train
ml-train:
	@$(PYTHON) -m ml.train_fraud_model
	@echo "[OK] Modèle entraîné — active-le avec : export SCORING_ENGINE=ml"

# ─────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────
.PHONY: test
test:
	@$(PYTHON) tests/test_e2e.py

# Tests unitaires ML (features, entraînement, repli sur règles) : sans Docker.
.PHONY: test-ml
test-ml:
	@$(PYTHON) tests/test_ml_model.py

.PHONY: smoke
smoke:
	@echo "Smoke tests :"
	@echo "  Postgres :"
	@docker exec stripe-postgres psql -U $(PG_USER) -d $(PG_DB) -c "SELECT count(*) FROM merchants;" 2>/dev/null
	@echo "  Mongo :"
	@docker exec stripe-mongo mongosh --quiet --eval "db.getSiblingDB('$(MONGO_DB)').transaction_logs.countDocuments()"
	@echo "  Redis :"
	@docker exec stripe-redis redis-cli -a $(REDIS_PASSWORD) --no-auth-warning PING
	@echo "  Kafka :"
	@docker exec stripe-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | wc -l | xargs echo "  Topics count:"
	@echo "  Debezium :"
	@curl -fsS $(KAFKA_CONNECT_URL)/connectors/stripe-postgres-cdc-v2/status 2>/dev/null | python3 -c "import sys, json; d = json.load(sys.stdin); print(f\"  State: {d['connector']['state']}\")"

# ─────────────────────────────────────────────────────────
# Infrastructure as Code (cible AWS) — cf. terraform/README.md
# ─────────────────────────────────────────────────────────
.PHONY: tf-validate
tf-validate:
	@terraform -chdir=terraform fmt -recursive -check
	@for dir in bootstrap envs/dev envs/prod; do \
		terraform -chdir=terraform/$$dir init -backend=false -input=false >/dev/null && \
		terraform -chdir=terraform/$$dir validate || exit 1; \
	done
	@echo "[OK] Terraform valide (fmt + validate sur bootstrap, dev, prod)"

# ─────────────────────────────────────────────────────────
# Help
# ─────────────────────────────────────────────────────────
.PHONY: help
help:
	@echo "Stripe Polyglot - Commandes disponibles :"
	@echo ""
	@echo "  Bootstrap :"
	@echo "    make init          Génère .env, installe deps, démarre l'infra, initialise tout"
	@echo "    make init-env      Génère juste le .env"
	@echo "    make install       Installe les deps Python dans venv/"
	@echo ""
	@echo "  Lifecycle :"
	@echo "    make up            Démarre la stack"
	@echo "    make down          Arrête la stack (volumes OK)"
	@echo "    make clean         Arrête + supprime volumes (reset complet)"
	@echo "    make restart       down + up"
	@echo "    make logs          Tail logs de tous les services"
	@echo "    make status        Status rapide (conteneurs, topics, connecteurs)"
	@echo ""
	@echo "  Données :"
	@echo "    make seed          Insère merchants + customers + payment_methods"
	@echo "    make producer      Lance le producer live de transactions"
	@echo ""
	@echo "  Flink :"
	@echo "    make flink-build   (Re)build l'image Flink custom"
	@echo "    make flink-submit  Soumet le job PyFlink (sans rebuild)"
	@echo "    make flink         (re)build + submit"
	@echo ""
	@echo "  Dashboard :"
	@echo "    make dashboard     Lance Streamlit sur :8501"
	@echo ""
	@echo "  Snowflake :"
	@echo "    make snowflake-setup    Crée warehouse + schéma"
	@echo "    make snowflake-export   Lance l'extract quotidien"
	@echo ""
	@echo "  Tests :"
	@echo "    make test          Lance le test E2E"
	@echo "    make queries-check Exécute queries/*.sql et queries/*.js sur la stack"
	@echo "    make tf-validate   Vérifie terraform/ (fmt + validate, sans compte AWS)"
	@echo "    make smoke         Vérifie que tous les services répondent"
