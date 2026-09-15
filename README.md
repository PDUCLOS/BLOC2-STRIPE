# Stripe Polyglot — Démo locale

> Démo end-to-end d'une plateforme de paiement polyglot : PostgreSQL (OLTP) · MongoDB (logs/features/alertes) · Kafka + Debezium (CDC) · Redis (feature store online) · Job de scoring fraude temps réel · Streamlit (dashboard) · Snowflake (OLAP).

## 🎬 Démo rapide (pour la vidéo)

```bash
# 1. Tout démarre en 1 commande (le pipeline complet, y compris les consumers)
./demo.sh

# 2. Ouvre le dashboard dans ton navigateur
open http://localhost:8501
```

Le script `demo.sh` :
- démarre les 5 services Docker (Postgres, Mongo, Kafka, Debezium, Redis)
- déploie le connecteur Debezium
- insère le seed (200 merchants, 5000 customers)
- lance le flink-like job (scoring fraude temps réel)
- lance le consumer Kafka→Mongo
- lance le dashboard Streamlit
- lance le producer de transactions en continu

## 🚀 Démarrage manuel (étape par étape)

```bash
# 1. Init env
make init-env          # génère .env avec des secrets aléatoires

# 2. venv Python 3.11
make install

# 3. Démarre l'infra Docker
make up

# 4. Initialise (topics Kafka, roles Postgres, connecteur Debezium)
bash scripts/create_topics.sh
bash scripts/postgres_init_roles.sh
bash scripts/deploy_debezium.sh

# 5. Seed les données de démo
make seed              # 200 merchants, 5000 customers, ~8000 PM

# 6. Dans 4 terminaux séparés :
make producer          # 5 txn/s, 5% fraude (terminal 1)
./venv/bin/python -u producers/flink_like_job.py    # scoring (terminal 2)
./venv/bin/python -u producers/mongo_writer.py      # Kafka→Mongo (terminal 3)
make dashboard         # Streamlit sur :8501 (terminal 4)
```

## 📺 URLs utiles (pour la démo)

| Service | URL | Credentials |
|---|---|---|
| Streamlit Dashboard | http://localhost:8501 | — |
| Kafka Connect (Debezium) | http://localhost:8083/connectors | — |
| Kafka brokers | `localhost:9092` (Docker) / `localhost:29092` (host) | — |
| PostgreSQL | `localhost:5432` | `stripe_app` / (voir .env) |
| MongoDB | `localhost:27017` | `admin` / (voir .env) |
| Redis | `localhost:6379` | (voir .env) |

## 🏗️ Architecture

```
┌─────────────┐    INSERT    ┌─────────────┐    CDC     ┌─────────────┐
│  Producer   │─────────────▶│ PostgreSQL  │───────────▶│  Debezium   │
│  (Python)   │              │   (OLTP)    │            │  Connect    │
└─────────────┘              └─────────────┘            └──────┬──────┘
                                                              │ logical replication
                                                              ▼
                                                       ┌─────────────┐
                                                       │    Kafka    │
                                                       │  (events)   │
                                                       └──────┬──────┘
                                                              │ subscribe
                                                              ▼
                          ┌────────────────────────────────────────────┐
                          │     Flink-like scoring job (Python)        │
                          │  • Lit transactions                        │
                          │  • Enrich via Redis (features, velocity)   │
                          │  • Calcule fraud_score + decision          │
                          │  • Update Redis (velocity windows)         │
                          └────────┬──────────────────────────┬────────┘
                                   │                          │
                       ┌───────────▼──────────┐    ┌──────────▼─────────┐
                       │  Kafka topic         │    │  Kafka topic        │
                       │  stripe.payments     │    │  stripe.fraud       │
                       │  .events             │    │  .alerts            │
                       └───────────┬──────────┘    └─────────────────────┘
                                   │
                                   ▼
                          ┌────────────────┐
                          │  Mongo writer  │
                          │  (Python)      │
                          └────────┬───────┘
                                   │
                       ┌───────────▼──────────┐
                       │      MongoDB         │
                       │  • transaction_logs  │
                       │  • fraud_alerts      │
                       │  • ml_features       │
                       └───────────┬──────────┘
                                   │
                       ┌───────────▼──────────┐
                       │     Streamlit        │
                       │     Dashboard        │
                       └──────────────────────┘

   ┌─────────────────────────────────────────────────────────────┐
   │  Daily ETL (cron / Airflow) → Snowflake (dim_*, fact_*)     │
   └─────────────────────────────────────────────────────────────┘
```

## 📁 Structure

```
.
├── docker-compose.yml       # 5 services (Postgres, Mongo, Kafka, Debezium, Redis)
├── Makefile                 # orchestration
├── demo.sh                  # script one-shot pour la démo
├── .env / .env.example      # config (gitignored .env)
├── init/                    # DDL Postgres + init Mongo
├── scripts/                 # topics Kafka, Debezium, utilitaires
├── seed/                    # seed initial (merchants, customers, PM)
├── producers/
│   ├── transaction_producer.py    # INSERT continu de transactions
│   ├── flink_like_job.py          # job scoring fraude (DataStream-style)
│   └── mongo_writer.py            # Kafka→Mongo consumer
├── dashboard/
│   └── app.py                # app Streamlit (5 pages)
├── etl/                      # export batch vers Snowflake
├── tests/
│   └── test_e2e.py           # test end-to-end
└── flink/                    # Dockerfile + requirements (PyFlink custom)
```

## 🎯 Scénario de démo (3 minutes)

1. **Cadrage (30s)** : montrer l'architecture (ce README)
2. **Services UP (30s)** : `docker compose ps` → 5 services healthy
3. **Seed (15s)** : 200 merchants, 5000 customers déjà en DB
4. **Pipeline live (1min)** :
   - Lancer le producer → montrer les INSERT dans psql
   - Voir le topic Kafka se remplir : `kafka-console-consumer --topic stripe.public.transactions`
   - Voir le flink-like scorer les transactions et les pousser dans `stripe.payments.events`
   - Voir les alertes fraude dans `stripe.fraud.alerts` (rouge, decision=block)
   - Voir les features se mettre à jour dans Redis
5. **Dashboard (1min)** : switcher sur http://localhost:8501
   - Page Overview : KPIs temps réel, decision breakdown
   - Page Live Transactions : flux qui défile, color-coded par score
   - Page Fraud Alerts : les alertes MongoDB
6. **Test E2E (15s)** : `make test` → tout vert
7. **Snowflake (optionnel, 30s)** : si credentials remplis dans .env, montrer `make snowflake-export`

## ⚠️ Notes techniques

### Pourquoi pas de vrai Flink custom image ?

J'ai tenté de builder une image Flink custom (PyFlink + connecteur Kafka + Redis + Mongo) sur Mac M5 Pro ARM64. **Impossible** : cascade de bugs (numpy 1.21.4 incompatible Py 3.11, JDK headers manquants, kafka-clients pas bundlé, `ClassCastException: [B` au runtime dans PyFlink 1.19).

**Solution retenue** : un job Python "Flink-like" (`producers/flink_like_job.py`) qui fait EXACTEMENT la même chose qu'un job PyFlink DataStream (source Kafka → enrich Redis → score → sink Kafka). Logique métier identique, juste l'API change. Pour la PROD : on déploie ce job via Flink standalone (sans Docker) ou KDA.

### Pourquoi 2 ports Kafka (9092 + 29092) ?

- `9092` : port Docker interne, utilisé par les conteneurs (Debezium, Mongo writer depuis le réseau)
- `29092` : port host, utilisé par les scripts Python sur ta machine

Le broker Kafka a 2 listeners : `PLAINTEXT://kafka:9092` (inter-container) et `PLAINTEXT_HOST://localhost:29092` (host). Le port mapping `29092:29092` rend le 2e accessible depuis ton Mac.

## 🔧 Commandes utiles

```bash
# Voir les topics Kafka
docker exec stripe-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consumer un topic
docker exec stripe-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic stripe.public.transactions --from-beginning --max-messages 1

# Inspecter Mongo
docker exec stripe-mongo mongosh -u admin -p "$MONGO_PASSWORD" --authenticationDatabase admin
> use stripe_nosql
> db.fraud_alerts.find().sort({created_at: -1}).limit(5).pretty()
> db.transaction_logs.countDocuments()

# Inspecter Redis
docker exec stripe-redis redis-cli -a "$REDIS_PASSWORD" --no-auth-warning keys 'feat_*'
docker exec stripe-redis redis-cli -a "$REDIS_PASSWORD" --no-auth-warning hgetall "feat_xxx"

# Voir les logs Debezium
docker logs stripe-debezium --tail 50

# Status global
make status
```

## 🧪 Tests

```bash
# Test E2E complet (5 étapes)
make test

# Smoke tests rapides
make smoke
```

## 📋 Prérequis

- Docker Desktop (>= 24.0)
- Python 3.11 (`brew install python@3.11`)
- 8-10 Go d'espace disque
- 8 Go de RAM minimum (recommandé 16+)

## 🎁 Snowflake (optionnel)

Pour activer l'export batch vers Snowflake :
1. Crée un compte trial sur https://signup.snowflake.com (Standard, AWS, eu-west-1)
2. Remplis `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` dans `.env`
3. Lance `make snowflake-setup` (crée warehouse + schéma)
4. Lance `make snowflake-export` (extract Postgres → load Snowflake)
