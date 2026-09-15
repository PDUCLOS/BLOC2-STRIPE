# Stripe Polyglot — Makefile
# Cœur de l'orchestration pour la démo

PROJECT_ROOT := $(shell pwd)
COMPOSE := docker compose --env-file .env
PYTHON := ./venv/bin/python
PIP := ./venv/bin/pip

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
	@if [ ! -d "venv" ]; then \
		/opt/homebrew/bin/python3.11 -m venv venv; \
		echo "[OK] venv créé (Python 3.11)"; \
	else \
		echo "[OK] venv existe déjà"; \
	fi
	@$(PIP) install --upgrade pip

.PHONY: install
install: venv
	@$(PIP) install -r requirements.txt
	@echo "[OK] Dépendances Python installées dans venv/"

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
seed:
	@$(PYTHON) seed/seed_data.py
	@echo "[OK] Seed terminé"

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
	@curl -fsS $(KAFKA_CONNECT_URL)/connectors/stripe-postgres-cdc/status 2>/dev/null | python3 -c "import sys, json; d = json.load(sys.stdin); print(f\"  State: {d['connector']['state']}\")"

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
	@echo "    make smoke         Vérifie que tous les services répondent"
