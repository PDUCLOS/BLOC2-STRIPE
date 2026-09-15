# Stripe Polyglot — Démo locale

> Démo end-to-end d'une plateforme de paiement polyglot : PostgreSQL (OLTP) · MongoDB (logs/features/alertes) · Kafka + Debezium (CDC) · Redis (feature store online) · Scoring fraude temps réel (règles + XGBoost) · MLflow + Evidently (tracking/monitoring ML) · Streamlit (dashboard) · Airflow (orchestration batch).
>
> **Snowflake (OLAP) n'est PAS connecté dans cette démo** — aucun compte
> trial configuré. `etl/load_snowflake.py` tourne en dry-run (affiche ce
> qui serait chargé, n'écrit rien) ; le schéma star existe (`make
> snowflake-setup`) mais n'est jamais peuplé. Voir "Snowflake (optionnel)" plus bas.

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
make init-env          # génère .env (secrets aléatoires + login démo du dashboard)

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
| Streamlit Dashboard | http://localhost:8501 | admin / Bloc2-Demo-2026 (démo locale) |
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
├── queries/                  # livrable 8 : requêtes SQL (OLTP, OLAP Snowflake) et MongoDB
├── terraform/                # IaC de la cible AWS (modules + envs dev/prod), validée en CI
└── docs/                     # PRESENTATION, ARCHITECTURE, sécurité, ML, MLOps, FinOps, OLAP, NoSQL
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
   - Écran de login : se connecter avec `admin` / `Bloc2-Demo-2026` (identifiants de démo locale ; seul le hash SHA-256 est dans `.env`, blocage après 5 échecs).
   - Onglet Vue d'ensemble : KPIs temps réel, transactions suspectes, alertes consolidées depuis MongoDB.
   - Onglet Performance ML : dérive, précision/rappel servis, comparaison règles vs XGBoost.
6. **Test E2E (15s)** : `make test` pour montrer que le parcours complet est vérifiable automatiquement.
7. **Snowflake (optionnel, 30s)** : si les credentials sont renseignés dans `.env`, lancer `make snowflake-export` pour montrer l'étape analytique batch.

## Notes techniques

### Pourquoi pas de vrai Flink custom image ?

J'ai tenté de builder une image Flink custom (PyFlink + connecteur Kafka + Redis + Mongo) sur Mac M5 Pro ARM64. **Impossible** : cascade de bugs (numpy 1.21.4 incompatible Py 3.11, JDK headers manquants, kafka-clients pas bundlé, `ClassCastException: [B` au runtime dans PyFlink 1.19).

**Solution retenue** : un job Python "Flink-like" (`producers/flink_like_job.py`) qui fait EXACTEMENT la même chose qu'un job PyFlink DataStream (source Kafka → enrich Redis → score → sink Kafka). Logique métier identique, juste l'API change. Pour la PROD : on déploie ce job via Flink standalone (sans Docker) ou KDA.

**Commentaire précis** : cette décision est un compromis de démonstration locale (stabilité sur Mac ARM64) et non une limitation de l'architecture cible en production.

### Projet sur Google Drive : attention au venv

Le dépôt local est dans un dossier Google Drive. Si les fichiers de `.venv/`
sont en « streaming » (non disponibles hors connexion), un `import` Python
peut bloquer indéfiniment : c'est ce qui est arrivé à `producers/mongo_writer.py`,
figé sur `import kafka` sans aucun message. Deux solutions : marquer `.venv/`
« Disponible hors connexion » dans Google Drive, ou créer le venv hors du
Drive (`python3.11 -m venv ~/.venvs/stripe-bloc2`) et y pointer `PYTHON` dans
le Makefile.

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
# Test E2E complet (16 contrôles : Postgres, Redis, Mongo, pipeline + fraud_indicators)
make test

# Exécute toutes les requêtes de queries/ sur la stack (échoue à la première erreur)
make queries-check

# Vérifie le code Terraform sans compte AWS (fmt + validate)
make tf-validate

# Smoke tests rapides
make smoke
```

**Commentaire précis** : exécuter `make test` avant la présentation permet de prouver l'intégrité end-to-end sans dépendre uniquement d'une démonstration visuelle.

## CI/CD & MLOps

[![CI](https://github.com/PDUCLOS/BLOC2-STRIPE/actions/workflows/ci.yml/badge.svg)](https://github.com/PDUCLOS/BLOC2-STRIPE/actions/workflows/ci.yml)

GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml))
monte la stack complète sur le runner à chaque push/PR (pas de mocks) et
lance `make test`, `make queries-check` et `make notebook-check` (audit
données/ML) ; un job séparé valide `terraform/` (`fmt` + `validate`). Cycle de vie
du modèle, monitoring, réentraînement automatique et incidents documentés :
**[docs/MLOPS.md](docs/MLOPS.md)**.

```bash
# Reproduire la CI en local — mêmes commandes, pas de réimplémentation
make init && make test && make notebook-check
```

## Infrastructure as Code (Terraform)

La cible cloud présentée en soutenance est écrite en Terraform dans
[`terraform/`](terraform/README.md) : VPC 3 AZ, RDS PostgreSQL Multi-AZ, MSK,
ElastiCache, MongoDB Atlas en PrivateLink, ECS Fargate, MWAA, S3, KMS,
Secrets Manager, alarmes et budget. Le code est **validé** (`make tf-validate`,
job CI `terraform`) mais **jamais appliqué** : aucun compte AWS n'est rattaché
au projet. Coûts estimés et leviers d'optimisation : [docs/FINOPS.md](docs/FINOPS.md).

## Requêtes SQL et NoSQL

| Fichier | Moteur | Exécuté par `make queries-check` |
|---|---|---|
| [`queries/postgres_oltp.sql`](queries/postgres_oltp.sql) | PostgreSQL 16 : revenu, décisions fraude, précision servie, RFM, vélocité, vues matérialisées, EXPLAIN, RGPD sous ROLLBACK | Oui |
| [`queries/mongodb_queries.js`](queries/mongodb_queries.js) | MongoDB 7 : alertes, règles déclenchées, logs, feature store, monitoring ML, index TTL | Oui |
| [`queries/snowflake_olap.sql`](queries/snowflake_olap.sql) | Snowflake : schéma en étoile, fenêtres, Dynamic Table | Non (Snowflake en dry-run) |

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
5. Exécute `snowsql -f queries/snowflake_olap.sql` pour les requêtes analytiques

### Passer à un compte Snowflake payant (production)

Le compte d'essai suffit pour une démonstration (30 jours, 400 $ de crédits).
Pour un usage réel, voici les étapes et ce qui change dans le projet :

1. **Compte** : édition *Enterprise* (requise pour le masquage dynamique des
   colonnes sensibles et la rétention Time Travel de 90 jours), hébergée sur
   AWS `eu-west-1`, dans la même région que la cible Terraform pour éviter
   les frais de sortie de données.
2. **Authentification par paire de clés** au lieu du mot de passe : générer une
   clé RSA, l'associer à un utilisateur de service (`ALTER USER ... SET
   RSA_PUBLIC_KEY`), stocker la clé privée dans AWS Secrets Manager (lue par
   le DAG MWAA) et remplacer `SNOWFLAKE_PASSWORD` par le chemin de la clé dans
   `etl/load_snowflake.py`.
3. **Moindre privilège** : un rôle `LOADER` (écriture sur le schéma
   analytique, utilisé par Airflow) et un rôle `ANALYST` (lecture seule), à la
   place du rôle d'administration utilisé en essai.
4. **Réseau** : *network policy* n'autorisant que les IP de sortie des NAT
   Gateway AWS, ou AWS PrivateLink (édition *Business Critical*).
5. **Coûts** : warehouse X-Small, `AUTO_SUSPEND = 60`, *resource monitor*
   plafonnant les crédits mensuels. Environ 180 $/mois pour l'export
   quotidien et un usage BI modéré (détail dans [docs/FINOPS.md](docs/FINOPS.md)).
6. **Pré-agrégats** : créer la Dynamic Table `dt_daily_revenue`
   (`queries/snowflake_olap.sql` §5).
7. **Orchestration** : le DAG `dags/stripe_daily_etl.py` s'exécute tel quel
   sur MWAA (module `terraform/modules/airflow`), avec les secrets Snowflake
   dans Secrets Manager.

Tant que ces étapes ne sont pas faites, Snowflake reste en dry-run et n'est
pas présenté comme branché au pipeline live.
