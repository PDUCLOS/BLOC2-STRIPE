# Stripe Polyglot — Documentation complète du projet

> **Bloc 2 — Certification Jedha Architecte en intelligence artificielle (RNCP41993)**
> Démo end-to-end d'une plateforme de paiement polyglot avec détection de fraude temps réel.
>
> Cette doc est l'**index central** du projet. Chaque section a des liens rapides (TOC) et
> chaque fichier est décrit avec : rôle, logique interne, dépendances.

---

## Table des matières — Liens rapides

### 1. Vue d'ensemble
- [1.1 — Pitch en 30 secondes](#11--pitch-en-30-secondes)
- [1.2 — Stack technique](#12--stack-technique)
- [1.3 — URLs utiles](#13--urls-utiles)

### 2. Architecture & flux de données
- [2.1 — Schéma global](#21--schéma-global)
- [2.2 — Logique du pipeline de fraude](#22--logique-du-pipeline-de-fraude)
- [2.3 — Topics Kafka & collections](#23--topics-kafka--collections)

### 3. Arborescence des fichiers (avec liens)
- [3.1 — Racine du projet](#31--racine-du-projet)
- [3.2 — `producers/`](#32--producers--génération--scoring--persistance)
- [3.3 — `flink/`](#33--flink--job-pyspark-de-production)
- [3.3bis — `ml/`](#33bis--ml--entraînement-inférence-et-monitoring-du-modèle-fraude)
- [3.3ter — `dags/`](#33ter--dags--orchestration-airflow)
- [3.4 — `dashboard/`](#34--dashboard--streamlit)
- [3.5 — `seed/`](#35--seed--données-initiales)
- [3.6 — `etl/`](#36--etl--export-snowflake)
- [3.7 — `init/postgres/`](#37--initpostgres--ddl-postgresql)
- [3.8 — `init/mongo/`](#38--initmongo--collections-mongodb)
- [3.9 — `scripts/`](#39--scripts--bash-utilitaires)
- [3.10 — `config/`](#310--config--debezium)
- [3.11 — `tests/`](#311--tests--end-to-end)
- [3.12 — `docs/`](#312--docs--documentation)
- [3.13 — Volumes Docker](#313--volumes-docker-nommés-pas-de-bind-mount)
- [3.14 — `common/`, `venv/`, `__pycache__/`](#314--dossiers-secondaires)

### 4. Logique interne de chaque fichier
- [4.1 — `transaction_producer.py`](#41--transaction_producerpy--le-générateur)
- [4.2 — `flink_like_job.py`](#42--flink_like_jobpy--le-scorer-fraude)
- [4.3 — `mongo_writer.py`](#43--mongo_writerpy--le-persistor)
- [4.4 — `dashboard/app.py`](#44--dashboardapppy--le-visualisateur)
- [4.5 — `seed_data.py`](#45--seed_datapy--linitialisation)
- [4.6 — `tests/test_e2e.py`](#46--test_e2epy--les-tests)
- [4.7 — `etl/load_snowflake.py`](#47--etlload_snowflakepy--letl-batch)
- [4.8 — `etl/snowflake_setup.py`](#48--etlsnowflake_setuppy--le-schéma-snowflake)
- [4.9 — `flink/fraud_scoring_job.py`](#49--flinkfraud_scoring_jobpy--le-job-prod)
- [4.10 — `_env.py`](#410--_envpy--le-chargeur-denv-universel)
- [4.11 — `ml/train_fraud_model.py`](#411--mltrain_fraud_modelpy--lentraînement)
- [4.12 — `ml/scoring.py`](#412--mlscoringpy--linférence)
- [4.13 — `ml/monitor.py`](#413--mlmonitorpy--le-monitoring)

### 5. Conventions & bonnes pratiques du repo
- [5.1 — Convention de nommage des fichiers](#51--convention-de-nommage-des-fichiers)
- [5.2 — Pattern d'import du .env](#52--pattern-dimport-du-env)
- [5.3 — Pattern de gestion des erreurs](#53--pattern-de-gestion-des-erreurs)
- [5.4 — Pattern de log formaté](#54--pattern-de-log-formaté)
- [5.5 — Pattern de signal handler](#55--pattern-de-signal-handler-gracieux)

### 6. Template pour nouveau fichier
- [6.1 — Checklist avant création](#61--checklist-avant-création)
- [6.2 — Template Python (producer / consumer / job)](#62--template-python-producer--consumer--job)
- [6.3 — Template Python (script SQL/DDL/Mongo)](#63--template-python-script-sqlddlmongo)
- [6.4 — Template Bash](#64--template-bash)
- [6.5 — Template SQL (init)](#65--template-sql-init)
- [6.6 — Template JSON (config)](#66--template-json-config)

### 7. Commandes & debugging
- [7.1 — Commandes make](#71--commandes-make)
- [7.2 — Inspection manuelle des services](#72--inspection-manuelle-des-services)
- [7.3 — Reset complet](#73--reset-complet)

---

## 1. Vue d'ensemble

### 1.1 — Pitch en 30 secondes

Plateforme **polyglotte** qui simule un système de paiement Stripe :
- **OLTP** : PostgreSQL 16 (source de vérité des transactions)
- **CDC** : Debezium capte chaque INSERT/UPDATE et le pousse dans Kafka
- **Stream processing** : un job de scoring fraude (PyFlink en prod, "Flink-like" Python en démo) lit Kafka, enrichit avec **Redis** (vélocité + features), calcule un `fraud_score` (règles statiques ou **modèle XGBoost entraîné**, selon `SCORING_ENGINE`), décide `allow / review / block`, et écrit dans 2 topics Kafka.
- **NoSQL** : un consumer Python lit les transactions scorées et les persiste dans **MongoDB** (`transaction_logs`, `fraud_alerts` avec TTL RGPD 90j, `ml_features`).
- **ML** : `ml/train_fraud_model.py` entraîne le modèle sur l'historique Postgres et trace le run dans **MLflow** ; `ml-monitor` (Evidently) surveille en continu drift et performance, et redéclenche l'entraînement automatiquement en cas de dérive.
- **Visualisation** : un dashboard **Streamlit** live (2 onglets) branche sur PG + Mongo + Redis + les métriques de monitoring ML.
- **OLAP** : un DAG **Airflow** (ou `make snowflake-export` en manuel) exporte les transactions vers **Snowflake** (star schema : `fact_transactions` + 5 dimensions).

### 1.2 — Stack technique

| Couche | Techno | Rôle |
|---|---|---|
| **OLTP** | PostgreSQL 16 | Source de vérité (merchants, customers, txns) |
| **Bus événementiel** | Kafka 7.6 (KRaft) | Topic CDC + sinks applicatifs |
| **CDC** | Debezium 2.6 Connect | Capture les changements PG → Kafka |
| **Stream processing** | PyFlink 1.18 (prod) / Python "flink-like" (démo) | Scoring fraude temps réel |
| **Feature store online** | Redis 7 | Vélocité 1h/24h, features client |
| **NoSQL** | MongoDB 7 | Logs, alertes, features ML |
| **Machine Learning** | XGBoost 2.0 | Modèle de scoring fraude entraîné (fallback règles si absent) |
| **ML tracking** | MLflow 2.14 | Runs d'entraînement, registre de modèles |
| **ML monitoring** | Evidently 0.4 | Drift + performance live, réentraînement auto |
| **Visualisation** | Streamlit 1.32 | Dashboard live, 2 onglets (Vue d'ensemble + Performance ML) |
| **OLAP** | Snowflake | Star schema, batch quotidien |
| **Orchestration** | Apache Airflow 2.9 (profil optionnel) / Makefile | DAG ETL quotidien |

### 1.3 — URLs utiles

| Service | URL | Credentials |
|---|---|---|
| Streamlit Dashboard | http://localhost:8501 | admin / Bloc2-Demo-2026 (démo locale) |
| MLflow (tracking + registre) | http://localhost:5001 | — |
| Airflow (si profil `airflow`) | http://localhost:8090 | admin / généré au 1er démarrage (`docker logs stripe-airflow`) |
| Kafka Connect (Debezium) | http://localhost:8083/connectors | — |
| Flink UI (si profil `flink`) | http://localhost:8081 | — |
| Kafka brokers | `localhost:9092` (Docker) / `localhost:29092` (host) | — |
| PostgreSQL | `localhost:5432` | `stripe_app` (ou `analytics_reader` en lecture seule) / `.env` |
| MongoDB | `localhost:27017` | `admin` / `.env` |
| Redis | `localhost:6379` | `.env` |

---

## 2. Architecture & flux de données

### 2.1 — Schéma global

```
[Producer Python]
       │ INSERT
       ▼
[PostgreSQL 16] ──OLTP── (merchants, customers, transactions)
       │ Logical replication (pgoutput)
       ▼
[Debezium Connect] ──CDC── capture chaque changement
       │
       ▼
[Kafka 7.6] ──stripe.public.transactions, .refunds, .fraud_indicators
       │ subscribe (consumer group "flink-fraud-scorer")
       ▼
[PyFlink / Flink-like job]
       │ ├─ Source : stripe.public.transactions
       │ ├─ Map : enrich Redis (velocity 1h/24h, features)
       │ ├─ Map : score (5 règles) + decision (allow/review/block)
       │ ├─ Sink 1 : stripe.payments.events
       │ ├─ Sink 2 : stripe.fraud.alerts (review/block only)
       │ └─ Write-back : UPDATE transactions.fraud_score
       ▼
[Mongo writer] ──Lit stripe.payments.events
       │ ├─ transaction_logs (TTL 90j)
       │ ├─ fraud_alerts (decision review/block)
       │ └─ logs (monitoring)
       ▼
[Streamlit Dashboard] ──Lit PG + Mongo + Redis live
       │
       ▼
[Airflow / cron] ──Quotidien 02:00 UTC
       │
       ▼
[Snowflake OLAP] ──fact_transactions + 5 dim_*
```

### 2.2 — Logique du pipeline de fraude

Le **scoring** a deux moteurs, choisis par `SCORING_ENGINE` : **XGBoost** (`ml`, modèle `xgboost-v1`, cf. [ML_INTEGRATION_STRATEGY.md](ML_INTEGRATION_STRATEGY.md)) avec **repli automatique** sur le moteur à règles **`rule-based-v1`** tant qu'aucun modèle n'est entraîné. Le moteur à règles (défaut, 5 règles à poids additifs) :

| Règle | Code | Condition | Score |
|---|---|---|---|
| R1 | `R1_high_amount` | `amount > 100_000` centimes (> 1000€) | +0.35 |
| R2 | `R2_card_testing` | `0 < amount < 200` AND device ∈ {pos, mobile} | +0.15 |
| R3 | `R3_high_risk_geo` | `ip_country ∈ {RU, NG, KP, IR, VE, BY}` | +0.40 |
| R4 | `R4_velocity_1h` | `velocity_1h > 10` | +0.25 |
| R5 | `R5_velocity_24h` | `velocity_24h > 50` | +0.15 |

**Décisions** (basées sur `FRAUD_SCORE_THRESHOLD` + `REVIEW_THRESHOLD`) :
- `score >= 0.85` → `block`
- `score >= 0.60` → `review`
- sinon → `allow`

**Vélocité** (Redis sorted set) :
- `v1h_<customer_id>` : ZSET des timestamps des 60 dernières minutes, TTL 3700s
- `v24h_<customer_id>` : ZSET des timestamps des 24 dernières heures, TTL 86500s
- Cardinalité (`zcard`) = nb de transactions dans la fenêtre

### 2.3 — Topics Kafka & collections

**Topics Kafka** (cf. `scripts/create_topics.sh`) :

| Topic | Partitions | Rétention | Source / Sink | Contenu |
|---|---|---|---|---|
| `stripe.public.transactions` | auto (Debezium) | 168h | Debezium CDC | Tous les INSERT/UPDATE sur `transactions` |
| `stripe.public.refunds` | auto | 168h | Debezium CDC | Refunds |
| `stripe.public.fraud_indicators` | auto | 168h | Debezium CDC | Indicateurs fraude |
| `stripe.payments.events` | 12 | 30j | Flink-like sink | Txns scorées (toutes) |
| `stripe.fraud.alerts` | 3 | 30j | Flink-like sink | Txns avec decision ∈ {review, block} |
| `stripe.etl.dead-letter` | 3 | ∞ | Tous les consumers | Messages malformés / erreurs de scoring |

**Collections MongoDB** (cf. `init/mongo/01_init_collections.js`) :

| Collection | TTL | Usage | Remplie par |
|---|---|---|---|
| `transaction_logs` | 90j (RGPD) | Log append-only de toutes les txns scorées | `mongo_writer.py` |
| `user_interactions` | 30j | Clics, navigations, échecs d'auth | (futur) |
| `ml_features` | — | Features pré-calculées pour ML | (futur) |
| `customer_feedback` | — | Disputes, contestations | (futur) |
| `fraud_alerts` | — | Alertes fraude (decision review/block) | `mongo_writer.py` |
| `logs` | — (TTL via `ttl_expires_at`) | Logs monitoring | `mongo_writer.py` |

---

## 3. Arborescence des fichiers (avec liens)

> Tous les chemins sont relatifs à la racine du projet.
> Les liens pointent vers les fichiers réels.

### 3.1 — Racine du projet

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `README.md` | Doc | Doc d'entrée — quickstart, architecture, démo | [→](../README.md) |
| `ARCHITECTURE.md` (ce fichier) | Doc | Doc technique détaillée | [→](./ARCHITECTURE.md) |
| `PRESENTATION.md` | Doc | Slides de présentation soutenance | [→](./PRESENTATION.md) |
| `Makefile` | Build | Orchestration : `make init`, `up`, `down`, `seed`, `producer`, `dashboard`, `ml-train`, `test`, `flink`, `snowflake-*` | [→](../Makefile) |
| `docker-compose.yml` | Infra | 8 services par défaut : postgres, mongo, kafka, debezium, redis, dashboard, **mlflow**, **ml-monitor** ; + profils optionnels `flink` (jobmanager/taskmanager) et `airflow` | [→](../docker-compose.yml) |
| `demo.sh` | Script | Démo one-shot (pour la vidéo) | [→](../demo.sh) |
| `requirements.txt` | Dépendances | Déps Python locales (producer, dashboard, scripts) | [→](../requirements.txt) |
| `.env` | Config (gitignored) | Variables d'environnement runtime | [→](../.env) |
| `.env.example` | Config template | Template pour `.env` (jamais de secrets en dur) | [→](../.env.example) |
| `.gitignore` | Git | Fichiers à ignorer | [→](../.gitignore) |
| `_env.py` | Module | Chargeur `.env` universel (cherché par sys.path) | [→](../_env.py) |

### 3.2 — `producers/` — Génération + scoring + persistance

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `transaction_producer.py` | Producer | Génère des transactions continues (5/s, 5% fraude) | [→](../producers/transaction_producer.py) |
| `flink_like_job.py` | Stream job | **Scoring fraude** temps réel — règles ou modèle ML selon `SCORING_ENGINE` | [→](../producers/flink_like_job.py) |
| `mongo_writer.py` | Consumer | Lit Kafka `stripe.payments.events` → MongoDB (`transaction_logs`, `fraud_alerts`, `ml_features`) | [→](../producers/mongo_writer.py) |

### 3.3 — `flink/` — Job PyFlink de production

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `Dockerfile` | Infra | Image Flink 1.18 + Python 3.11 + PyFlink + connecteurs | [→](../flink/Dockerfile) |
| `fraud_scoring_job.py` | Stream job | **Job prod** PyFlink DataStream (équivalent du flink-like) | [→](../flink/fraud_scoring_job.py) |
| `requirements.txt` | Dépendances | Déps PyFlink (différent de la racine) | [→](../flink/requirements.txt) |

> Le profil `flink` n'est **pas activé en démo** (ARM64 Mac → bugs de build). En démo on utilise
> le `flink_like_job.py` qui a la même logique métier.

### 3.3bis — `ml/` — Entraînement, inférence et monitoring du modèle fraude

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `features.py` | Module | Feature engineering partagée entraînement/inférence (même vecteur des deux côtés) | [→](../ml/features.py) |
| `train_fraud_model.py` | Script | Entraîne XGBoost sur l'historique Postgres, trace le run dans MLflow, sauvegarde le `.pkl` | [→](../ml/train_fraud_model.py) |
| `scoring.py` | Module | Charge le modèle (cache mémoire) et prédit — utilisé par `flink_like_job.py` | [→](../ml/scoring.py) |
| `monitor.py` | Script | Boucle continue : drift Evidently + performance live + réentraînement auto | [→](../ml/monitor.py) |
| `Dockerfile` | Infra | Image du service `ml-monitor` | [→](../ml/Dockerfile) |
| `models/` | Généré (gitignored) | `.pkl` + `.meta.json` produits par `make ml-train` | — |

### 3.3ter — `dags/` — Orchestration Airflow

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `stripe_daily_etl.py` | DAG | Export quotidien Postgres → Snowflake (02:00 UTC), profil Docker `airflow` | [→](../dags/stripe_daily_etl.py) |

### 3.4 — `dashboard/` — Streamlit

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `app.py` | App web | Dashboard live, 2 onglets : Vue d'ensemble (KPIs, charts, alertes) + Performance ML (drift, recall, distribution par modèle) | [→](../dashboard/app.py) |
| `Dockerfile` | Infra | Image Python 3.11-slim + Streamlit, containerise le dashboard (service `dashboard`, port 8501) | [→](../dashboard/Dockerfile) |

### 3.5 — `seed/` — Données initiales

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `seed_data.py` | Script | Insère 200 merchants + 5000 customers + ~8000 payment methods | [→](../seed/seed_data.py) |

### 3.6 — `etl/` — Export Snowflake

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `snowflake_setup.py` | Script | Crée warehouse + DB + schéma + tables (star schema) | [→](../etl/snowflake_setup.py) |
| `load_snowflake.py` | ETL batch | Extract PG → upsert dimensions → merge fact_transactions | [→](../etl/load_snowflake.py) |

### 3.7 — `init/postgres/` — DDL PostgreSQL

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `01_ddl.sql` | SQL | DDL : 6 tables + triggers + publication Debezium + vues matérialisées | [→](../init/postgres/01_ddl.sql) |

### 3.8 — `init/mongo/` — Collections MongoDB

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `01_init_collections.js` | Mongo shell | Crée 5 collections + index TTL RGPD | [→](../init/mongo/01_init_collections.js) |
| `02_app_user.js` | Mongo shell | Crée l'utilisateur applicatif `stripe_app` | [→](../init/mongo/02_app_user.js) |

### 3.9 — `scripts/` — Bash utilitaires

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `init_env.sh` | Bash | Génère `.env` avec secrets aléatoires (inclut `PG_ANALYTICS_PASSWORD`) | [→](../scripts/init_env.sh) |
| `create_topics.sh` | Bash | Crée les 3 topics applicatifs Kafka | [→](../scripts/create_topics.sh) |
| `postgres_init_roles.sh` | Bash | Crée `replication_user` (Debezium) + `analytics_reader` (dashboard, lecture seule) | [→](../scripts/postgres_init_roles.sh) |
| `deploy_debezium.sh` | Bash | Déploie le connecteur Debezium (POST `/connectors`) | [→](../scripts/deploy_debezium.sh) |

### 3.10 — `config/` — Debezium

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `debezium-connector.json` | JSON config | Template du connecteur Debezium PG (substitué par `deploy_debezium.sh`) | [→](../config/debezium-connector.json) |
| `debezium-connector-commentaire.md` | Doc | Explication ligne par ligne du JSON ci-dessus — un fichier `.json` ne peut pas contenir de commentaires natifs, ce doc compagnon comble le manque | [→](../config/debezium-connector-commentaire.md) |

### 3.11 — `tests/` — End-to-end

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `test_e2e.py` | Test | 16 tests : PG, Redis, Mongo, pipeline E2E (nécessite la stack Docker vivante) | [→](../tests/test_e2e.py) |
| `test_ml_model.py` | Test | Tests du module `ml/` : features, cycle train/save/load, fallback rule-based — **sans Docker** (données synthétiques) | [→](../tests/test_ml_model.py) |

### 3.12 — `docs/` — Documentation

| Fichier | Type | Rôle | Lien |
|---|---|---|---|
| `PRESENTATION.md` | Doc | Narratif complet du projet — architecture, choix techniques, limites, soutenance | [→](./PRESENTATION.md) |
| `ARCHITECTURE.md` (ce fichier) | Doc | Référence technique fichier par fichier | [→](./ARCHITECTURE.md) |
| `SECURITY_COMPLIANCE_PLAN.md` | Doc | Sécurité, RGPD/PCI-DSS, IAM, monitoring — étend la section RGPD de PRESENTATION.md | [→](./SECURITY_COMPLIANCE_PLAN.md) |
| `ML_INTEGRATION_STRATEGY.md` | Doc | Stratégie ML détaillée : feature store, entraînement, déploiement, monitoring | [→](./ML_INTEGRATION_STRATEGY.md) |
| `OLAP_SCHEMA_DESIGN.md` | Doc | Star schema Snowflake, clustering, stratégies d'agrégation et d'optimisation | [→](./OLAP_SCHEMA_DESIGN.md) |
| `NOSQL_DATA_MODEL.md` | Doc | Schéma MongoDB collection par collection, relations, stratégie d'indexation | [→](./NOSQL_DATA_MODEL.md) |

### 3.13 — Volumes Docker (nommés, pas de bind mount)

> Tous les volumes de données sont des **volumes Docker nommés**
> (`postgres-data`, `mongo-data`, `redis-data`, `kafka-data`, `debezium-data`,
> `flink-data`, `mlflow-data`, `airflow-data`) — plus de dossier `./data/`
> bind-monté. Ce choix évite deux classes de bugs rencontrées en pratique :
> les bind mounts cassent sous virtiofs (Docker Desktop macOS, cf. §4.2 de
> PRESENTATION.md) et sous FUSE (Google Drive, cf. §4.9 de PRESENTATION.md) —
> WiredTiger (Mongo) et Kafka refusent tous deux d'écrire sur ces systèmes de
> fichiers. `docker volume ls | grep stripe-polyglot` pour les lister ; ne
> jamais les éditer à la main, seul Docker doit y écrire.

### 3.14 — Dossiers secondaires

| Dossier | Contenu | À savoir |
|---|---|---|
| `common/` | Vide (sauf `.DS_Store` + `__pycache__/`) | Ancien emplacement d'`env.py` — **n'est plus utilisé**, le helper est maintenant à la racine (`_env.py`) |
| `venv/` | Virtualenv Python 3.11 | Créé par `make install` ou `demo.sh`. **À gitignorer**. |
| `__pycache__/` | Cache Python bytecode | **À gitignorer**. Régénéré automatiquement. |

---

## 4. Logique interne de chaque fichier

> Pour chaque fichier, je décris : **rôle**, **logique** (étapes), **dépendances**,
> **points d'attention**.

### 4.1 — [`transaction_producer.py`](../producers/transaction_producer.py) — Le générateur

**Rôle** : Génère un flux continu de transactions simulées et les INSERT dans PostgreSQL.

**Logique** (boucle infinie avec `--rate` txns/s, `--fraud-ratio` 0-1) :
1. Charge le `.env` via `import _env` (sys.path ajoute la racine).
2. Construit la config PG depuis `PG_HOST`/`PG_DB`/etc.
3. Pour chaque tick (1 / `--rate` sec) :
   - Décide `is_fraud = random.random() < --fraud-ratio`.
   - Si pas de burst en cours : pioche un merchant + (customer, pm) selon le segment (`new`/`inactive` pour fraude, `standard`/`premium` pour normal).
   - 30% des fraudes déclenchent un **burst** de 12-20 txns rapides (card testing simulé).
   - Construit la transaction avec `build_transaction(is_fraud)` :
     - `is_fraud=True` → 70% gros montant (100-2000€), 30% petit (1-5€) pour card testing.
     - 50% des fraudes vont dans un pays à risque (`RU`/`NG`/`KP`/`IR`/`VE`).
     - 30% des fraudes utilisent `device=pos` (atypique).
   - INSERT dans `transactions` (Debezium capte).
4. **Signal handler** : `SIGINT`/`SIGTERM` → `_running = False` (arrêt gracieux).
5. Stats affichées tous les 25 txns.

**Points d'attention** :
- `metadata.is_fraud_pattern` = `True` est volontairement mis dans le JSONB → permet de mesurer
  la **rappel** du modèle (combien de fraudes réelles sont détectées).
- Le burst crée de la vélocité côté Redis → c'est ce qui déclenche la règle R4.

### 4.2 — [`flink_like_job.py`](../producers/flink_like_job.py) — Le scorer fraude

**Rôle** : Lit en continu le topic `stripe.public.transactions` (CDC Debezium), calcule le
`fraud_score` pour chaque transaction, et pousse le résultat dans 2 topics Kafka.

**Logique** :
1. Connexions : Redis (feature store) + Kafka Consumer + Kafka Producer + Postgres (write-back optionnel).
2. Consumer Kafka : `group.id=flink-fraud-scorer`, `auto.offset.reset=earliest`, subscribe `stripe.public.transactions`.
3. Boucle `consumer.poll(1.0)` :
   - **Parse error** (JSON malformé, raw vide) → DLQ `stripe.etl.dead-letter` + continue.
   - **Scoring** via `score_transaction(txn, r)` :
     - **Vélocité Redis** : ZADD timestamp courant dans `v1h_<cid>` et `v24h_<cid>`, ZREMRANGEBYSCORE pour expirer, EXPIRE 3700/86500s, ZCARD pour le compte.
     - **Features** : HSET `feat_<cid>` (last_amount, last_country, v1h, v24h).
     - **5 règles** (cf. §2.2) : `score += 0.35 / 0.15 / 0.40 / 0.25 / 0.15`, min(round, 4, 1.0).
     - **Décision** : `>= 0.85 → block`, `>= 0.60 → review`, sinon `allow`.
     - Champs ajoutés : `fraud_score`, `decision`, `velocity_1h`, `velocity_24h`, `rules_triggered`, `model_version` (`"rule-based-v1"` ou `"xgboost-v1"`), `scored_at`.
   - **Sink 1** : `stripe.payments.events` (toutes les txns scorées).
   - **Sink 2** : `stripe.fraud.alerts` (decision ∈ review/block).
   - **Write-back PG** (une seule transaction) : `UPDATE transactions SET fraud_score = ... WHERE txn_id = ... AND fraud_score IS NULL`, puis, si la ligne a bien été modifiée et que la décision est `review`/`block`, `INSERT INTO fraud_indicators (txn_id, anomaly_score, rules_triggered, model_version, decision)`. Le filtre `IS NULL` rend l'ensemble idempotent : un message CDC rejoué (ou l'événement généré par ce même UPDATE) ne produit ni second score ni indicateur en double.
4. Stats tous les 25 messages (count, alerts, writebacks, rate).
5. Signal handler : arrêt gracieux.

**Points d'attention** :
- **Score min(round, 4, 1.0)** → cap à 1.0 max.
- Le write-back PG peut faire boucler Debezium → OK, le consumer a `auto.offset.reset=earliest`
  seulement au 1er démarrage, et on ne fait l'UPDATE que si `fraud_score IS NULL`.
- Les `velocity_1h`/`velocity_24h` sont **écrites** avant d'être lues (sliding window).

### 4.3 — [`mongo_writer.py`](../producers/mongo_writer.py) — Le persistor

**Rôle** : Lit le topic `stripe.payments.events` et écrit dans MongoDB.

**Logique** :
1. Connexions : MongoDB + Kafka Consumer + Kafka Producer (DLQ).
2. Subscribe `stripe.payments.events`, group `mongo-writer`.
3. Pour chaque message :
   - **Parse JSON** (sinon DLQ).
   - INSERT dans `transaction_logs` (champs : `txn_id`, `event_type="transaction.scored"`, `payload`, `source`, `created_at`).
   - INSERT dans `logs` (monitoring, TTL 90j via `ttl_expires_at`).
   - Si `decision ∈ {review, block}` → INSERT dans `fraud_alerts` (champs dénormalisés pour la query rapide).
4. Stats tous les 50 messages.

**Points d'attention** :
- `fraud_alerts` n'a pas de TTL → on garde l'historique des alertes (nécessaire pour audit).
- `transaction_logs` a un TTL de 90j côté Mongo (cf. `01_init_collections.js`).
- DLQ via `stripe.etl.dead-letter` (topic rétention infinie).

### 4.4 — [`dashboard/app.py`](../dashboard/app.py) — Le visualisateur

**Rôle** : Dashboard Streamlit temps réel branché sur PG + Mongo + Redis.

**Logique** :
1. Page config (titre, icône, layout wide, sidebar expanded) + CSS custom (Stripe purple `#635BFF`).
2. Connexions cachées 3s (`@st.cache_resource(ttl=3)`) : `get_pg`, `get_redis`, `get_mongo`.
3. **Sidebar** :
   - Slider auto-refresh (1-30s).
   - Status services (PG/Redis/Mongo up/down).
4. **Header** : titre + dernière mise à jour.
5. **KPIs** (5 colonnes) :
   - Total transactions, txns 1h, revenus €, alertes fraude (delta = fraud_rate), score moyen.
6. **Charts row 1** :
   - Time series (30min) : total + fraud stacked.
   - Bar chart horizontal : top 10 pays fraude.
7. **Charts row 2** :
   - Top 8 marchands par GMV (gradient Stripe).
   - Liste alertes MongoDB (rouge pour `block`, orange pour `review`).
8. **Tableau** : 15 dernières transactions suspectes (score >= 0.6).
9. **Expander** : schéma ASCII de l'architecture.
10. `time.sleep(refresh)` + `st.rerun()` → boucle de refresh.

**Points d'attention** :
- `@st.cache_resource(ttl=3)` → reconnexion toutes les 3s, crucial pour le live.
- `FRAUD_SCORE_THRESHOLD` vient du `.env`.
- `stripe_app` user Mongo n'a pas le droit d'admin → on utilise l'URI avec `mongodb://user:pass@host:port/`.

### 4.5 — [`seed_data.py`](../seed/seed_data.py) — L'initialisation

**Rôle** : Insère 200 merchants + 5000 customers + ~8000 payment methods (idempotent).

**Logique** :
1. Charge `.env` via `import _env`.
2. Ouvre une connexion PG, TRUNCATE CASCADE (merchants, customers, payment_methods, transactions, refunds, fraud_indicators) `RESTART IDENTITY`.
3. **Insert merchants** : 200 lignes, `execute_values` (batch insert performant).
4. **Insert customers** : 5000 lignes, `execute_values`, email unique.
5. **Insert payment_methods** : 2 par client en moyenne, random `card`/`wallet`/`bank_transfer`, `is_default` sur 40% des premiers.
6. Counts finaux affichés.

**Points d'attention** :
- `Faker.seed(42)` + `random.seed(42)` → seed déterministe.
- `ON CONFLICT DO NOTHING` sur merchants/customers (sécurité).
- `page_size=1000` sur `execute_values` pour PM (perf).

### 4.6 — [`test_e2e.py`](../tests/test_e2e.py) — Les tests

**Rôle** : 16 tests end-to-end sur les 4 couches.

**Catégories** :

| Section | Tests |
|---|---|
| **PostgreSQL** (8 tests) | Connexion, schéma 6 tables, données seed, insert/read txn, idempotency key unique, publication Debezium, vues matérialisées, `amount=BIGINT` |
| **Redis** (3 tests) | Connexion, velocity ZSET, feature store HSET |
| **MongoDB** (4 tests) | Connexion, collections, schema fraud_alerts, index TTL RGPD |
| **Pipeline E2E** (1 test) | Insertion txn 1500€ + pays RU → `fraud_score` doit remonter en 10s |

**Logique** :
- Pattern : `check(name, fn)` capture `AssertionError` → résultat PASS/FAIL.
- Cleanup en `finally` (DELETE les txns insérées par le test).
- Test pipeline optionnel : skip si le job tourne pas → remontée en `AssertionError` clair.

### 4.7 — [`etl/load_snowflake.py`](../etl/load_snowflake.py) — L'ETL batch

**Rôle** : Extract PG → upsert dimensions → merge fact_transactions (quotidien).

**Logique** :
1. Charge `.env` via `import _env`.
2. Si `SNOWFLAKE_*` manquant → `sys.exit(1)` avec message clair.
3. **`extract_from_pg(target_date)`** : SELECT toutes les txns `succeeded` du jour (LEFT JOIN PM pour type/brand).
4. **`upsert_dimensions(sf_cur, rows)`** : MERGE `dim_merchant` + INSERT `dim_customer` / `dim_payment_method` (idempotent grâce à `WHERE NOT EXISTS`).
5. **`load_to_snowflake(rows, target_date)`** :
   - Batches de 5000 (perf).
   - Calcul `fee_amount = amount_eur * 0.014` (simule la commission Stripe).
   - Calcul `processing_ms` (aléatoire déterministe par `txn_id`).
   - MERGE INTO `fact_transactions` (clé `txn_id`).
6. Si `SNOWFLAKE_ACCOUNT` vide → **dry-run** (log seulement).

**Points d'attention** :
- Les jointures `dim_*.X = src.X` dans le MERGE résolvent les `*_key` (surrogate keys).
- `decimal.handling.mode=double` côté Debezium + Snowflake `NUMBER(18,2)` → précision conservée.

### 4.8 — [`etl/snowflake_setup.py`](../etl/snowflake_setup.py) — Le schéma Snowflake

**Rôle** : Crée warehouse + DB + schéma + tables (star schema) + pré-peuple `dim_date` et `dim_geography`.

**Logique** :
1. Vérif `SNOWFLAKE_ACCOUNT`/`USER`/`PASSWORD` dans `.env` (sinon exit 1).
2. Connexion + cur.
3. **Idempotence** : `run(cur, sql, label)` swallow l'erreur "already exists".
4. CREATE WAREHOUSE `STRIPE_WH` (X-SMALL, AUTO_SUSPEND=60s).
5. CREATE DATABASE `STRIPE_DWH` + SCHEMA `PROD`.
6. CREATE TABLE : 5 dimensions (`dim_date`, `dim_merchant`, `dim_customer`, `dim_payment_method`, `dim_geography`) + 1 fait (`fact_transactions` CLUSTER BY `date_key, merchant_key`).
7. Pré-peuple `dim_date` (2020-2029 via `GENERATOR(ROWCOUNT=>3653)`).
8. Pré-peuple `dim_geography` (17 pays dont 5 high-risk).

**Points d'attention** :
- `CLUSTER BY (date_key, merchant_key)` sur la fait = pruning massif pour les requêtes analytiques.
- Les `dim_*` ont des `valid_from`/`is_current` → prépare le **SCD type 2** (en prod).

### 4.9 — [`flink/fraud_scoring_job.py`](../flink/fraud_scoring_job.py) — Le job prod

**Rôle** : **Équivalent PyFlink** du `flink_like_job.py` (pour le profil `flink` Docker).

**Différences avec le flink-like** :
- Utilise l'API `StreamExecutionEnvironment` (DataStream).
- `KafkaSource.builder()` au lieu de `Consumer` confluent-kafka.
- `FraudScoringFunction` = `MapFunction` (PyFlink).
- `FraudAlertFilter` = `FilterFunction`.
- 2 `KafkaSink` (events + alerts).
- 1 `Write-back` Postgres par transaction (pas par batch).

**Déclenchement** : `make flink` (= `flink-build` + `flink-submit`).

### 4.10 — [`_env.py`](../_env.py) — Le chargeur d'env universel

**Rôle** : Trouve et charge le `.env` dans `os.environ`, utilisé par **tous** les scripts Python.

**Logique** :
1. `_find_env_file()` cherche un `.env` :
   - 1) `Path.cwd() / .env`
   - 2) Remonter depuis `__file__` (cherche dans chaque parent).
2. Si trouvé :
   - Essaie `from dotenv import load_dotenv` (déjà installé).
   - Fallback : parse manuel (gère quotes + commentaires).

**Pattern d'usage** (à mettre en haut de **chaque** script) :
```python
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parent.parent if _P(__file__).parent.name != "tests" else _P(__file__).resolve().parent.parent))
import _env  # noqa: F401
```

> **Pourquoi pas un `common/env.py` ?** Parce que ce `sys.path.insert(0, ...)` ne marche
> pas toujours quand le script est lancé depuis un autre CWD. `_env.py` à la racine est
> trouvable par n'importe quel script.

---

### 4.11 — [`ml/train_fraud_model.py`](../ml/train_fraud_model.py) — L'entraînement

**Rôle** : Entraîne le modèle XGBoost de scoring fraude et trace le run dans MLflow.

**Logique** :
1. `extract_training_data()` — requête Postgres avec vélocité recalculée par sous-requête corrélée (même sémantique que Redis en live) + label `is_fraud` = `metadata->>'is_fraud_pattern'` (la vérité terrain posée par le générateur, **pas** la décision du moteur à règles — sinon le modèle réapprendrait juste les seuils des règles au lieu de détecter la fraude réellement injectée)
2. `build_dataset()` — vectorise via `ml/features.py` (même fonction que l'inférence)
3. `temporal_train_test_split()` — split chronologique 80/20 (pas aléatoire, pour éviter une fuite d'information via la corrélation temporelle des clients)
4. `train_model()` — XGBoost avec `scale_pos_weight` calculé pour compenser le déséquilibre de classes
5. Le tout tourne dans un `mlflow.start_run()` : params, métriques, et le modèle (`mlflow.xgboost.log_model(..., registered_model_name="fraud-detector")`) sont tracés ensemble
6. Sauvegarde locale : `ml/models/fraud_xgboost-v1.pkl` + `.meta.json` (repris par `ml/scoring.py` côté inférence)

**Dépendances** : Postgres (source), MLflow (tracking — fallback fichier local si `MLFLOW_TRACKING_URI` absent).

**Point d'attention** : `extract_current_window()`, dans ce même fichier, est réutilisée par `ml/monitor.py` — même requête, bornée sur une fenêtre récente au lieu de tout l'historique.

---

### 4.12 — [`ml/scoring.py`](../ml/scoring.py) — L'inférence

**Rôle** : Charge le modèle entraîné et calcule un `fraud_score`, appelé depuis `flink_like_job.py` quand `SCORING_ENGINE=ml`.

**Logique** :
1. `load_model()` — chargement paresseux, mis en cache en mémoire au premier appel (pas de relecture disque à chaque transaction)
2. Si le fichier `.pkl` n'existe pas (pas encore entraîné) : renvoie `None`, jamais d'exception — c'est ce `None` que `flink_like_job.py` interprète comme "retombe sur les règles"
3. `score()` — construit le vecteur de features (`ml/features.py`) et renvoie `model.predict_proba(...)[0, 1]`

**Point d'attention** : le cache mémoire ne se rafraîchit jamais tant que le process tourne — un réentraînement (manuel ou via `ml/monitor.py`) écrase le fichier, mais un scorer déjà lancé continue sur l'ancienne version jusqu'à son redémarrage (limite documentée, cf. `PRESENTATION.md` §3.8).

---

### 4.13 — [`ml/monitor.py`](../ml/monitor.py) — Le monitoring

**Rôle** : Boucle continue qui détecte la dérive du modèle et déclenche un réentraînement automatique. Tourne dans le service Docker `ml-monitor`.

**Logique** (`check_once()`, appelée toutes les `ML_MONITOR_INTERVAL_SECONDS`) :
1. Récupère la référence (`extract_training_data()` + le même split que l'entraînement) et la fenêtre courante (`extract_current_window()`)
2. `compute_drift()` — Evidently `DataDriftPreset`, extrait `share_of_drifted_columns`
3. `compute_live_performance()` — applique le modèle actuel aux données fraîches, calcule precision/recall/f1 contre la vérité terrain
4. Si `drift_share > ML_DRIFT_THRESHOLD` OU `recall < ML_MIN_RECALL` : appelle `train_fraud_model.main()` directement (import Python, pas un sous-process) — protégé par un cooldown pour ne pas réentraîner à chaque cycle si le problème persiste
5. Écrit un document dans `MongoDB.ml_monitoring` à chaque cycle — c'est la source de données de l'onglet "Performance ML" du dashboard

**Dépendances** : Postgres (données), MongoDB (écriture du statut), MLflow (tracking des réentraînements déclenchés), `ml/train_fraud_model.py` (import direct).

---

## 5. Conventions & bonnes pratiques du repo

### 5.1 — Convention de nommage des fichiers

| Type | Pattern | Exemple |
|---|---|---|
| Producer / consumer / job | `snake_case.py` | `transaction_producer.py` |
| Script utilitaire | `snake_case.py` | `seed_data.py` |
| Test | `test_*.py` | `test_e2e.py` |
| Bash | `snake_case.sh` | `create_topics.sh` |
| SQL init | `NN_name.sql` (ordre alpha) | `01_ddl.sql` |
| Mongo init | `NN_name.js` | `01_init_collections.js` |
| Config JSON | `kebab-case.json` | `debezium-connector.json` |
| Doc | `UPPER_CASE.md` ou `kebab-case.md` | `ARCHITECTURE.md`, `PRESENTATION.md` |

### 5.2 — Pattern d'import du `.env`

Tous les scripts Python qui touchent aux services commencent par :

```python
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parent.parent if _P(__file__).parent.name != "tests" else _P(__file__).resolve().parent.parent))
import _env  # noqa: F401
```

**Pourquoi** : le `parent.parent` est la racine du projet où se trouve `_env.py`. Le `if` spécial
sur `tests/` permet aux tests d'importer depuis `/tests/` sans casser le path.

### 5.3 — Pattern de gestion des erreurs

Deux écoles coexistent :

**A) Fail-fast + exit 1** (pour les jobs critiques, producer, mongo_writer) :
```python
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Erreur {context}: {e}", file=sys.stderr)
        sys.exit(1)
```

**B) DLQ + continue** (pour le flink-like job, sur parse/score error) :
```python
try:
    scored = score_transaction(txn, r)
except Exception as e:
    dlq_payload = json.dumps({"error": f"scoring_error: {e}", ...}).encode("utf-8")
    producer.produce("stripe.etl.dead-letter", value=dlq_payload)
    producer.poll(0)
    print(f"  [DLQ] scoring error at offset {msg.offset()}: {e}", file=sys.stderr)
    count += 1
    continue
```

### 5.4 — Pattern de log formaté

Tous les scripts utilisent des préfixes texte en majuscules entre crochets (pas d'emoji, pour rester lisible en CI/audit) :
- `[OK]` succès
- `[ERROR]` erreur fatale
- `[WARN]` avertissement non-bloquant
- `[WAIT]` attente
- `[START]` démarrage
- `[STOP]` arrêt
- `[INFO]` information
- `  → ` sous-étape (flèche conservée pour décrire un flux/une transition)
- `  [DLQ]` message envoyé en DLQ
- `  [BURST]` burst de fraude détecté

Format typique : `f"  [{count:6d} txns, {fraud:4d} fraud] rate={rate:.1f}/s"`

### 5.5 — Pattern de signal handler gracieux

Tous les long-running scripts (producer, flink-like, mongo_writer) :

```python
_running = True

def signal_handler(sig, frame):
    global _running
    print("\n[STOP] Stopping {nom}...")
    _running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Boucle principale
while _running:
    # ... travail
    # Sortie gracieuse à la fin de l'itération
```

> Ne **jamais** faire de `time.sleep(60)` en dehors de la condition `if _running:`.

---

## 6. Template pour nouveau fichier

### 6.1 — Checklist avant création

Avant d'écrire un nouveau fichier, demande-toi :

- [ ] **Où va-t-il ?** (parmi `producers/`, `etl/`, `dashboard/`, `seed/`, `tests/`, `scripts/`, `init/`)
- [ ] **Quel type ?** (Python / Bash / SQL / Mongo shell / JSON / Markdown)
- [ ] **A-t-il besoin du `.env` ?** → si oui, inclure le pattern `_env` (cf. §5.2)
- [ ] **Est-il long-running ?** → si oui, signal handler (cf. §5.5)
- [ ] **A-t-il besoin d'une DLQ ?** → si oui, topic `stripe.etl.dead-letter`
- [ ] **A-t-il besoin de retries sur erreur ?** → capturer + log + retry/backoff
- [ ] **A-t-il besoin de stats ?** → compteur + log tous les N
- [ ] **A-t-il besoin d'être idempotent ?** → TRUNCATE / UPSERT / MERGE / ON CONFLICT
- [ ] **Est-il testé ?** → ajouter dans `tests/test_e2e.py` ou un nouveau `test_*.py`
- [ ] **Est-il documenté dans cette ARCHITECTURE.md ?** → ajouter le lien en §3

### 6.2 — Template Python (producer / consumer / job)

```python
#!/usr/bin/env python3
"""
{NOM_DU_FICHIER} — {DESCRIPTION COURTE EN 1 LIGNE}.

{Description longue : ce que fait le script, dans quel contexte.}

Usage :
    {commande d'usage 1}
    {commande d'usage 2}
"""
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Dépendances tierces (psycopg2, redis, kafka, pymongo, etc.) ──────────────
import redis  # noqa: F401  (adapte)

# ── Chargeur .env universel ──────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

# ── Config depuis .env ────────────────────────────────────────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

# ── Constantes métier ─────────────────────────────────────────────────────────
HIGH_RISK_COUNTRIES = {"RU", "NG", "KP", "IR", "VE", "BY"}
FRAUD_THRESHOLD = float(os.environ.get("FRAUD_SCORE_THRESHOLD", 0.85))

# ── Signal handler gracieux ───────────────────────────────────────────────────
_running = True


def signal_handler(sig, frame):
    """Gère l'interruption du script (Ctrl+C ou SIGTERM) pour un arrêt gracieux."""
    global _running
    print("\n[STOP] Stopping {nom}...")
    _running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ── Fonctions métier (une par responsabilité) ────────────────────────────────
def connect_redis():
    """Connexion Redis avec gestion d'erreur."""
    r = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT,
        password=REDIS_PASSWORD or None,
        decode_responses=True, socket_timeout=2,
    )
    try:
        r.ping()
        return r
    except redis.RedisError as e:
        print(f"[ERROR] Redis connection failed: {e}")
        sys.exit(1)


def process_one(payload, r):
    """Traite UN événement métier.
    
    Args:
        payload (dict): événement désérialisé depuis Kafka/PG/autre.
        r (redis.Redis): connexion Redis (feature store).
        
    Returns:
        dict: résultat enrichi (score, decision, features...).
    """
    # ... logique ...
    return enriched


# ── Point d'entrée ────────────────────────────────────────────────────────────
def main():
    """Point d'entrée principal.
    
    1. Se connecte aux services.
    2. Boucle tant que _running.
    3. Traite chaque message.
    4. Stats tous les N.
    """
    print(f"[START] {NOM_DU_FICHIER} started")
    r = connect_redis()

    count = 0
    start = time.time()
    while _running:
        # ... poll Kafka, fetch PG, etc. ...
        # result = process_one(payload, r)
        count += 1

        if count % 25 == 0:
            rate = count / (time.time() - start)
            print(f"  [{count:6d}] rate={rate:.1f}/s")

    print(f"[OK] Stopped: {count} processed")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Erreur {nom}: {e}", file=sys.stderr)
        sys.exit(1)
```

### 6.3 — Template Python (script SQL/DDL/Mongo)

```python
#!/usr/bin/env python3
"""
{NOM} — {DESCRIPTION COURTE}.

Usage : {commande}
Prérequis : {variables d'env ou fichiers requis}
"""
import os
import sys
from pathlib import Path

# ── Chargeur .env ────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

# ── Config ────────────────────────────────────────────────────────────────────
PG_CONFIG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", 5432)),
    dbname=os.environ.get("PG_DB", "stripe_oltp"),
    user=os.environ.get("PG_USER", "stripe_app"),
    password=os.environ.get("PG_PASSWORD", ""),
)

# ── Fonctions ─────────────────────────────────────────────────────────────────
def connect():
    """Connexion au service."""
    # ...
    return connection


def run_action(conn):
    """Exécute l'action métier sur la connexion."""
    # ...


def main():
    """Point d'entrée principal."""
    print(f"[START] {NOM} started")
    conn = connect()
    try:
        run_action(conn)
        print(f"[OK] {NOM} terminé")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

### 6.4 — Template Bash

```bash
#!/usr/bin/env bash
# {NOM_DU_SCRIPT} — {DESCRIPTION COURTE}
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Charge .env
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    echo "[ERROR] .env manquant. Lance : make init-env"
    exit 1
fi
set -a
source "$PROJECT_ROOT/.env"
set +a

echo "[START] {NOM} started..."

# ── Étapes ────────────────────────────────────────────────────────────────────
# ...

echo "[OK] {NOM} terminé"
```

### 6.5 — Template SQL (init)

```sql
-- =====================================================================
-- {Nom du fichier} — {Description}
-- Exécuté automatiquement au 1er démarrage du conteneur {service}
-- =====================================================================

-- Extensions (si PG)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- {Tables, vues, publication, etc.}

-- Trigger updated_at (convention du repo)
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Indexes (toujours penser aux perfs)
CREATE INDEX idx_xxx ON table_name (column);
```

### 6.6 — Template JSON (config)

```json
{
  "name": "{connector-name}",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "replication_user",
    "database.password": "REPLACE_ME",
    "database.dbname": "stripe_oltp",
    "database.server.name": "stripe",
    "plugin.name": "pgoutput",
    "publication.name": "stripe_publication",
    "slot.name": "stripe_debezium_slot",
    "table.include.list": "public.table1,public.table2",
    "topic.prefix": "stripe",
    "snapshot.mode": "initial"
  }
}
```

> **JAMAIS** de secret en dur dans le JSON. Substituer via le script bash avec un heredoc Python.

---

## 7. Commandes & debugging

### 7.1 — Commandes `make`

| Commande | Rôle |
|---|---|
| `make init` | Bootstrap complet : `.env` + venv + deps + Docker + topics + Debezium |
| `make init-env` | Génère `.env` : secrets aléatoires + hash du login démo du dashboard |
| `make install` | Installe deps Python dans `venv/` |
| `make up` | Démarre la stack Docker |
| `make down` | Arrête (volumes conservés) |
| `make clean` | Arrête + supprime volumes (reset complet) |
| `make restart` | down + up |
| `make logs` | Tail logs de tous les services |
| `make status` | Status conteneurs + topics + connecteurs |
| `make seed` | Insère 200 merchants + 5000 customers + 8000 PM |
| `make producer` | Lance le producer de transactions |
| `make flink-build` | Build l'image Flink custom (profil `flink`) |
| `make flink-submit` | Soumet le job PyFlink |
| `make flink` | build + submit |
| `make dashboard` | Lance Streamlit sur :8501 (mode host, venv local) |
| `make snowflake-setup` | Crée warehouse + schéma Snowflake |
| `make snowflake-export` | Lance l'extract quotidien |
| `make test` | Lance les tests E2E |
| `make smoke` | Vérifie que tous les services répondent |
| `make help` | Affiche l'aide |

> Alternative containerisée du dashboard (sans venv host) :
> `docker compose up -d --build dashboard` → même URL http://localhost:8501, connecté à postgres/mongo/redis via le réseau Docker interne.

### 7.2 — Inspection manuelle des services

```bash
# Topics Kafka
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

### 7.3 — Reset complet

```bash
# 1. Arrêter tous les process Python
pkill -9 -f flink_like_job.py
pkill -9 -f mongo_writer.py
pkill -9 -f transaction_producer.py
pkill -9 -f "streamlit run"

# 2. Supprimer les volumes Docker
make clean

# 3. (Optionnel) Supprimer le venv
rm -rf venv/

# 4. Relancer tout
./demo.sh
```

---

## Notes finales

- **Mainteneur** : Patrice Duclos (RNCP41993 Architecte en intelligence artificielle)
- **Date de dernière mise à jour** : 2026-09-15
- **Statut** : Démo fonctionnelle, prête soutenance
- **Évolution future** :
  - Profil `flink` à stabiliser sur ARM64
  - ~~DAG Airflow~~ fait (`dags/stripe_daily_etl.py`)
  - ~~Modèle ML scoring~~ fait (XGBoost + MLflow + Evidently, cf. [MLOPS.md](MLOPS.md))
  - ~~Infrastructure as Code~~ écrite et validée en CI (`terraform/`), à appliquer sur un compte AWS
  - Debezium sur MSK Connect (plugin à packager) et Snowflake réel (compte payant)
  - Prometheus + Grafana pour l'observabilité
  - Schema Registry + Avro pour Kafka (au lieu de JSON)
