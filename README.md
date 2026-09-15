# Stripe Polyglot — Démo locale

> Démo end-to-end d'une plateforme de paiement polyglot : PostgreSQL (OLTP) · MongoDB (logs/features/alertes) · Kafka + Debezium (CDC) · Redis (feature store online) · Scoring fraude temps réel (règles + XGBoost) · MLflow + Evidently (tracking/monitoring ML) · Streamlit (dashboard) · Snowflake (OLAP) · Airflow (orchestration batch).

**Commentaire précis** : ce README sert de script de démonstration technique. L'ordre des sections suit le parcours réel d'exécution (quickstart -> pipeline live -> vérifications -> tests).

## Quickstart

```bash
git clone https://github.com/<ton-user>/<ton-repo>.git
cd <ton-repo>
./demo.sh                      # démarre tout le pipeline en 1 commande
# puis ouvre le dashboard :
#   http://localhost:8501
```

Prérequis : Docker Desktop ≥ 24, Python 3.11, ~10 Go de disque, 8 Go de RAM (16+ recommandé). Détails plus bas.

**Commentaire précis** : si la machine a moins de 16 Go RAM, fermer les apps lourdes avant `./demo.sh` pour éviter les ralentissements Kafka/Streamlit.

## Démo rapide (pour la vidéo)

```bash
# 1. Tout démarre en 1 commande (le pipeline complet, y compris les consumers)
./demo.sh

# 2. Ouvre le dashboard dans ton navigateur
open http://localhost:8501
```

Le script `demo.sh` :
- démarre les services Docker infra (Postgres, Mongo, Kafka, Debezium, Redis, MLflow)
- déploie le connecteur Debezium
- insère le seed (200 merchants, 5000 customers)
- lance le flink-like job (scoring fraude temps réel — règles par défaut, `SCORING_ENGINE=ml` après `make ml-train`)
- lance le consumer Kafka→Mongo
- lance le dashboard Streamlit
- lance le producer de transactions en continu

Pour activer le scoring ML complet (modèle entraîné + monitoring Evidently +
réentraînement auto) : `make ml-train` puis relancer le job de scoring avec
`SCORING_ENGINE=ml` et démarrer `docker compose up -d ml-monitor` — détaillé
dans `docs/ML_INTEGRATION_STRATEGY.md`.

**Commentaire précis** : l'intérêt principal de `demo.sh` est de garantir un démarrage reproductible pour la soutenance, sans oublis de dépendances intermédiaires.

## Démarrage manuel (étape par étape)

```bash
# 1. Initialise l'environnement local et génère les secrets de base utilisés par les services
make init-env          # génère .env avec des secrets aléatoires

# 2. Installe les dépendances Python dans un venv isolé pour éviter de polluer le système
make install

# 3. Démarre l'infrastructure commune: bases de données, Kafka, Debezium et Redis
make up

# 4. Exécute les initialisations techniques indispensables avant le flux temps réel
#    - topics Kafka pour publier les événements
#    - rôles Postgres pour autoriser le CDC
#    - connecteur Debezium pour capter les changements
bash scripts/create_topics.sh
bash scripts/postgres_init_roles.sh
bash scripts/deploy_debezium.sh

# 5. Charge les données de démonstration pour avoir un volume réaliste dès le départ
make seed              # 200 merchants, 5000 customers, ~8000 PM

# 6. Lance les composants applicatifs dans 4 terminaux séparés pour suivre chaque flux indépendamment
make producer          # 5 txn/s, 5% fraude (terminal 1)
./venv/bin/python -u producers/flink_like_job.py    # scoring (terminal 2) — SCORING_ENGINE=ml après make ml-train
./venv/bin/python -u producers/mongo_writer.py      # Kafka→Mongo (terminal 3)
make dashboard         # Streamlit sur :8501 (terminal 4)

# 7. (Optionnel) Entraîne et active le scoring ML
make ml-train                          # entraîne XGBoost, trace le run dans MLflow
docker compose up -d ml-monitor        # drift Evidently + réentraînement auto
```

## URLs utiles (pour la démo)

| Service | URL | Credentials |
|---|---|---|
| Streamlit Dashboard | http://localhost:8501 | — |
| MLflow (tracking + registre de modèles) | http://localhost:5001 | — |
| Airflow (si `docker compose --profile airflow up -d`) | http://localhost:8090 | admin / voir `docker logs stripe-airflow` |
| Kafka Connect (Debezium) | http://localhost:8083/connectors | — |
| Kafka brokers | `localhost:9092` (Docker) / `localhost:29092` (host) | — |
| PostgreSQL | `localhost:5432` | `stripe_app` (ou `analytics_reader` lecture seule) / (voir .env) |
| MongoDB | `localhost:27017` | `admin` / (voir .env) |
| Redis | `localhost:6379` | (voir .env) |

## Architecture

**Commentaire précis** : le flux critique est `PostgreSQL -> Debezium -> Kafka -> scoring -> Mongo/Redis`, qui matérialise la séparation OLTP (transactionnel) et NoSQL (lecture analytique temps réel).

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
   │  Airflow (DAG quotidien, 02:00 UTC) → Snowflake (dim_*, fact_*)│
   └─────────────────────────────────────────────────────────────┘

   ┌─────────────────────────────────────────────────────────────┐
   │  ml/train_fraud_model.py → MLflow (tracking + registre)      │
   │  ml-monitor (Evidently) → drift/perf → réentraînement auto   │
   └─────────────────────────────────────────────────────────────┘
```

## Structure

```
.
├── docker-compose.yml       # 8 services par défaut (Postgres, Mongo, Kafka, Debezium, Redis,
│                            #   Dashboard, MLflow, ml-monitor) + profils "flink"/"airflow"
├── Makefile                 # orchestration
├── demo.sh                  # script one-shot pour la démo
├── .env / .env.example      # config (gitignored .env)
├── init/                    # DDL Postgres (+ rôle analytics_reader) + init Mongo
├── scripts/                 # topics Kafka, Debezium, utilitaires
├── seed/                    # seed initial (merchants, customers, PM)
├── producers/
│   ├── transaction_producer.py    # INSERT continu de transactions
│   ├── flink_like_job.py          # scoring fraude — règles ou modèle ML (SCORING_ENGINE)
│   └── mongo_writer.py            # Kafka→Mongo consumer (+ ml_features)
├── ml/                       # entraînement (make ml-train), inférence, monitoring (Evidently+MLflow)
├── dags/                     # DAG Airflow (export quotidien Postgres → Snowflake)
├── dashboard/
│   ├── app.py                 # app Streamlit — 2 onglets : Vue d'ensemble + Performance ML
│   └── Dockerfile             # image du service `dashboard` (port 8501)
├── etl/                      # export batch vers Snowflake
├── tests/
│   ├── test_e2e.py           # test end-to-end (nécessite la stack Docker)
│   └── test_ml_model.py      # tests du module ml/ (sans Docker)
├── flink/                    # Dockerfile + requirements (PyFlink custom)
└── docs/                     # PRESENTATION, ARCHITECTURE, sécurité, ML, OLAP, NoSQL
```

## Scénario de démo (3 minutes)

**Commentaire précis** : ce scénario est ordonné pour prouver d'abord la fiabilité technique (services, flux, tests), puis la valeur métier (dashboard et alertes fraude).

1. **Cadrage (30s)** : montrer l'architecture pour expliquer le rôle de chaque brique avant de lancer le flux.
2. **Services UP (30s)** : `docker compose ps` pour prouver que l'infra est opérationnelle et stable.
3. **Seed (15s)** : vérifier que les données de référence sont déjà présentes dans la base avant les inserts temps réel.
4. **Pipeline live (1min)** :
   - Lancer le producer pour montrer qu'une transaction part de zéro et devient une ligne PostgreSQL.
   - Vérifier que Kafka reçoit bien les événements CDC via `kafka-console-consumer --topic stripe.public.transactions`.
   - Montrer que le job de scoring enrichit la transaction et publie le verdict dans `stripe.payments.events`.
   - Ouvrir `stripe.fraud.alerts` pour illustrer les cas bloqués et les alertes métier.
   - Contrôler Redis pour prouver que les features temps réel sont mises à jour en parallèle du scoring.
5. **Dashboard (1min)** : basculer sur http://localhost:8501 pour relier les données techniques à une lecture métier.
   - Page Overview : KPIs temps réel et répartition des décisions.
   - Page Live Transactions : flux en direct, avec un code couleur basé sur le score.
   - Page Fraud Alerts : alertes consolidées depuis MongoDB.
6. **Test E2E (15s)** : `make test` pour montrer que le parcours complet est vérifiable automatiquement.
7. **Snowflake (optionnel, 30s)** : si les credentials sont renseignés dans `.env`, lancer `make snowflake-export` pour montrer l'étape analytique batch.

## Notes techniques

### Pourquoi pas de vrai Flink custom image ?

J'ai tenté de builder une image Flink custom (PyFlink + connecteur Kafka + Redis + Mongo) sur Mac M5 Pro ARM64. **Impossible** : cascade de bugs (numpy 1.21.4 incompatible Py 3.11, JDK headers manquants, kafka-clients pas bundlé, `ClassCastException: [B` au runtime dans PyFlink 1.19).

**Solution retenue** : un job Python "Flink-like" (`producers/flink_like_job.py`) qui fait EXACTEMENT la même chose qu'un job PyFlink DataStream (source Kafka → enrich Redis → score → sink Kafka). Logique métier identique, juste l'API change. Pour la PROD : on déploie ce job via Flink standalone (sans Docker) ou KDA.

**Commentaire précis** : cette décision est un compromis de démonstration locale (stabilité sur Mac ARM64) et non une limitation de l'architecture cible en production.

### Pourquoi 2 ports Kafka (9092 + 29092) ?

- `9092` : port Docker interne, utilisé par les conteneurs (Debezium, Mongo writer depuis le réseau)
- `29092` : port host, utilisé par les scripts Python sur ta machine

Le broker Kafka a 2 listeners : `PLAINTEXT://kafka:9092` (inter-container) et `PLAINTEXT_HOST://localhost:29092` (host). Le port mapping `29092:29092` rend le 2e accessible depuis ton Mac.

## Commandes utiles

**Commentaire précis** : ces commandes sont pensées pour diagnostiquer rapidement les 4 zones à risque pendant la démo : Kafka (topics), Debezium (CDC), Mongo (persist), Redis (features).

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

## Tests

```bash
# Test E2E complet (5 étapes)
make test

# Smoke tests rapides
make smoke
```

**Commentaire précis** : exécuter `make test` avant la présentation permet de prouver l'intégrité end-to-end sans dépendre uniquement d'une démonstration visuelle.

## Prérequis

- Docker Desktop (>= 24.0)
- Python 3.11 (`brew install python@3.11`)
- 8-10 Go d'espace disque
- 8 Go de RAM minimum (recommandé 16+)

## Snowflake (optionnel)

Pour activer l'export batch vers Snowflake :
1. Crée un compte trial sur https://signup.snowflake.com (Standard, AWS, eu-west-1)
2. Remplis `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` dans `.env`
3. Lance `make snowflake-setup` (crée warehouse + schéma)
4. Lance `make snowflake-export` (extract Postgres → load Snowflake)
