# Index fichiers & fonctions — Stripe Polyglot

Référence à plat : chaque fichier du repo, son rôle en une ligne, et ses
fonctions/classes publiques avec ce qu'elles font. Pour l'explication
narrative (pourquoi ces choix), voir [PRESENTATION.md](PRESENTATION.md) ;
pour la doc technique complète fichier par fichier avec logique détaillée,
voir [ARCHITECTURE.md](ARCHITECTURE.md). Ce document est l'index de
recherche rapide — "où est la fonction X".

---

## Racine

### [`_env.py`](../_env.py)
Chargeur `.env` universel, importé en tête de **tous** les scripts Python du repo.

| Fonction | Rôle |
|---|---|
| `_find_env_file()` | Cherche `.env` : d'abord `cwd`, puis en remontant les parents de `__file__` |
| *(import seul)* | Charge le `.env` trouvé dans `os.environ` via `python-dotenv`, ou parsing manuel en repli |

---

## `seed/` — Données initiales

### [`seed_data.py`](../seed/seed_data.py)
Insère 200 merchants, 5000 customers, ~8000 payment_methods dans Postgres.

| Fonction | Rôle |
|---|---|
| `insert_merchants(cur)` | Génère et insère les marchands (Faker, seed=42 pour reproductibilité) |
| `insert_customers(cur)` | Idem clients — distribution de segments calibrée pour le volume de fraude ciblé |
| `insert_payment_methods(cur)` | Moyens de paiement par client (loi normale sur le nombre) |
| `main()` | `TRUNCATE ... CASCADE` puis réinsère tout — reset complet, pas d'ajout incrémental |

---

## `producers/` — Génération, scoring, persistance

### [`transaction_producer.py`](../producers/transaction_producer.py)
Génère un flux continu de transactions (95 % légitimes / 5 % de fraude tirée parmi trois profils : brutal, card testing, furtive) avec du bruit réaliste côté légitime.

| Fonction | Rôle |
|---|---|
| `pick_merchant(cur)` | Marchand actif aléatoire |
| `pick_customer_pm(cur, is_fraud)` | Cible les segments `new`/`inactive` si fraude (profil type) |
| `pick_fraud_profile()` | Tire le profil de fraude (`FRAUD_PROFILES` : brutal 35 %, card testing 25 %, furtive 40 %) |
| `build_transaction(is_fraud, profile)` | Construit le payload selon le profil — pose `metadata.is_fraud_pattern` (**vérité terrain** de `ml/train_fraud_model.py`) et `metadata.fraud_profile` |
| `insert_transaction(cur, txn)` | `INSERT` paramétré dans `transactions` |
| `main()` | Boucle continue ; rafales de fraude (card testing, brutal) et rafales légitimes (4 % des clients) |

### [`flink_like_job.py`](../producers/flink_like_job.py)
Scorer fraude temps réel — lit `stripe.public.transactions`, écrit `fraud_score`.

| Fonction | Rôle |
|---|---|
| `signal_handler(sig, frame)` | Arrêt propre sur Ctrl+C/SIGTERM |
| `score_transaction(txn, r)` | Cœur du scoring — vélocité Redis, puis règles OU `ml.scoring.score()` selon `SCORING_ENGINE` (repli automatique sur règles si modèle absent) |
| `main()` | `connect_pg()` interne (reconnexion limitée), boucle consommateur Kafka, write-back Postgres |

### [`mongo_writer.py`](../producers/mongo_writer.py)
Consommateur Kafka → MongoDB (`transaction_logs`, `fraud_alerts`, `ml_features`).

| Fonction | Rôle |
|---|---|
| `signal_handler(sig, frame)` | Arrêt propre |
| `get_mongo_client()` | Construit l'URI selon credentials présents ou non |
| `main()` | Consomme `stripe.payments.events`, écrit les 3 collections + DLQ si échec de parsing/écriture |

---

## `ml/` — Entraînement, inférence, monitoring

### [`features.py`](../ml/features.py)
Feature engineering **partagée** entraînement ↔ inférence — point critique pour éviter le skew training/serving.

| Fonction | Rôle |
|---|---|
| `build_feature_vector(amount, created_at, ip_country, device_type, velocity_1h, velocity_24h)` | Vecteur `[amount_log, hour_of_day, day_of_week, is_high_risk_country, is_pos_device, velocity_1h, velocity_24h]`, ordre figé (`FEATURE_NAMES`) |

### [`train_fraud_model.py`](../ml/train_fraud_model.py)
Entraîne XGBoost, trace le run dans MLflow. Point d'entrée : `make ml-train`.

| Fonction | Rôle |
|---|---|
| `extract_training_data()` | Requête Postgres, vélocité recalculée par sous-requête SQL corrélée, label = `metadata->>'is_fraud_pattern'` |
| `extract_current_window(minutes)` | Même requête que ci-dessus, bornée sur une fenêtre récente — **réutilisée par `ml/monitor.py`** |
| `build_dataset(df)` | DataFrame brut → matrice de features (via `ml.features.build_feature_vector`) + labels |
| `temporal_train_test_split(df, X, y, test_frac=0.2)` | Split **chronologique** (pas aléatoire) — évite la fuite d'info via la vélocité |
| `train_model(X_train, y_train)` | `XGBClassifier` avec `scale_pos_weight` calculé dynamiquement |
| `evaluate_model(model, X_test, y_test)` | precision/recall/f1/ROC AUC |
| `main()` | Orchestre tout, log MLflow (`mlflow.start_run`, `mlflow.xgboost.log_model`), sauvegarde `.pkl` |

### [`scoring.py`](../ml/scoring.py)
Inférence — appelée par `producers/flink_like_job.py` quand `SCORING_ENGINE=ml`.

| Fonction | Rôle |
|---|---|
| `load_model()` | Chargement paresseux, mis en cache en mémoire (pas de rechargement tant que le process tourne) |
| `score(amount, created_at, ip_country, device_type, velocity_1h, velocity_24h)` | Renvoie une probabilité, ou `None` si le modèle n'existe pas encore (signal de repli) |

### [`monitor.py`](../ml/monitor.py)
Boucle continue (service Docker `ml-monitor`) : dérive + performance **réellement servie** + réentraînement auto.

| Fonction | Rôle |
|---|---|
| `get_mongo_db()` | Connexion Mongo |
| `compute_drift(X_reference, X_current)` | Evidently `DataDriftPreset`, renvoie `drift_share` |
| `_parse_is_fraud(payload)` | Extrait `metadata.is_fraud_pattern` (gère les deux formats vus en pratique : dict natif ou string JSON) |
| `compute_served_performance(db, window_minutes)` | **Relit ce qui a été RÉELLEMENT décidé** en production (MongoDB `transaction_logs`) — pas une resimulation SQL. Corrige un angle mort découvert en conditions réelles (précision tombée à 0.29 sans que l'ancien check, basé sur SQL, ne le détecte) |
| `check_once(db, last_retrain_at)` | Un cycle complet : dérive + perf servie, déclenche `train_fraud_model.main()` si seuils franchis (délai de carence anti-boucle) |
| `main()` | Boucle infinie, `ML_MONITOR_INTERVAL_SECONDS` entre chaque cycle |

---

## `etl/` — Export Snowflake (compte d'essai branché le 25/09/2026 ; dry-run sans `SNOWFLAKE_ACCOUNT`)

### [`snowflake_setup.py`](../etl/snowflake_setup.py)
Bootstrap **manuel ponctuel** (`make snowflake-setup`) — jamais dans le DAG quotidien.

| Fonction | Rôle |
|---|---|
| `run(cur, sql, label)` | Exécute + log, ignore silencieusement les erreurs "already exists" (idempotence) |
| `main()` | Crée warehouse, DB, schéma, 5 dimensions + `fact_transactions`, pré-peuple `dim_date`/`dim_geography` |

### [`load_snowflake.py`](../etl/load_snowflake.py)
Export batch quotidien, appelé par le DAG Airflow.

| Fonction | Rôle |
|---|---|
| `extract_from_pg(target_date)` | Transactions `succeeded` du jour, JOIN `payment_methods` + `merchants` (nécessaire pour `upsert_dimensions`) |
| `upsert_dimensions(sf_cur, rows)` | Insère les nouvelles lignes `dim_merchant`/`dim_customer`/`dim_payment_method` **avant** le MERGE des faits — sans ça, les FK des faits seraient NULL |
| `create_staging(sf_cur)` | Table temporaire de transit du lot (le connecteur ne réécrit en multi-lignes qu'un `INSERT ... VALUES`) |
| `load_to_snowflake(rows, target_date)` | Par lot : `INSERT` dans la table de transit, dimensions par `INSERT ... WHERE NOT EXISTS`, puis `MERGE INTO fact_transactions` avec clés résolues par `LEFT JOIN` ; dry-run si `SNOWFLAKE_ACCOUNT` absent |
| `main()` | Extrait puis charge |

### [`refresh_views.py`](../etl/refresh_views.py)
Rafraîchit `mv_daily_revenue`/`mv_merchant_stats` (Postgres, créées `WITH NO DATA`).

| Fonction | Rôle |
|---|---|
| `main()` | `REFRESH MATERIALIZED VIEW CONCURRENTLY`, repli sans `CONCURRENTLY` au tout premier refresh |

---

## `dashboard/app.py` — Streamlit, 2 onglets

| Fonction | Rôle |
|---|---|
| `get_pg()`, `get_redis()`, `get_mongo()` | Connexions mises en cache (`st.cache_resource`, TTL 3s) |
| `pg_query(sql, params)` | Wrapper SQL générique, retourne un DataFrame vide en cas d'erreur (pas de crash dashboard) |
| `kpis_from_pg()` | 5 KPIs agrégés en une requête (`FILTER`) |
| `txn_over_time()` | Série temporelle 30 dernières minutes |
| `fraud_by_country()` | Top 10 pays par fraude |
| `top_merchants()` | Top 8 marchands par GMV |
| `fraud_alerts_mongo(limit)` | Alertes récentes MongoDB |
| `redis_stats(customer_ids)` | Vélocité live (mêmes clés que le scorer) |
| `recent_suspicious()` | 15 dernières transactions à score ≥ 0.6 |
| `ml_monitoring_latest()` / `ml_monitoring_history(limit)` | Lit `ml_monitoring` (écrite par `ml/monitor.py`) |
| `fraud_score_by_model_version(limit)` | Distribution des scores, comparaison `rule-based-v1` vs `xgboost-v1` |

Toutes les requêtes SQL sont `@st.cache_data(ttl=3)` — évite de re-requêter à chaque rerun Streamlit interne (widgets, etc.), pas seulement au vrai refresh minuté.

---

## `flink/fraud_scoring_job.py` — Job PyFlink (profil Docker `"flink"`, optionnel)

Équivalent logique de `producers/flink_like_job.py`, sur vrai cluster Flink.

| Fonction/Classe | Rôle |
|---|---|
| `FraudScoringFunction.open(runtime_context)` | Init Redis — dans `open()` et pas `__init__` (s'exécute côté TaskManager, pas côté client) |
| `FraudScoringFunction.map(raw)` | Même 5 règles que `flink_like_job.py`, + write-back Postgres |
| `FraudAlertFilter.filter(value)` | Filtre `review`/`block` pour le sink `stripe.fraud.alerts` |
| `main()` | Construit le graphe DataStream (source Kafka → map → 2 sinks) |

---

## `dags/stripe_daily_etl.py` — DAG Airflow (profil Docker `"airflow"`, optionnel)

| Tâche | Commande |
|---|---|
| `snowflake_export` | `python /opt/airflow/etl/load_snowflake.py` |
| `refresh_materialized_views` | `python /opt/airflow/etl/refresh_views.py` |

`snowflake_setup` volontairement absent (bootstrap manuel, cf. `etl/snowflake_setup.py`).

---

## `queries/` — Livrable 8 : requêtes SQL et NoSQL

Exécutées sur la stack par `make queries-check` (et en CI), sauf Snowflake.

| Fichier | Contenu | Données lues (source) |
|---|---|---|
| [`postgres_oltp.sql`](../queries/postgres_oltp.sql) | 9 requêtes : top marchands, décisions par moyen de paiement, précision/rappel servis, RFM, vélocité 1 h, remboursements en attente, vues matérialisées, EXPLAIN idempotence, RGPD sous ROLLBACK | `transactions` (producer + write-back scorer), `fraud_indicators` (scorer), `merchants`/`customers`/`payment_methods` (seed), `mv_*` (refresh_views) |
| [`mongodb_queries.js`](../queries/mongodb_queries.js) | 11 requêtes : alertes par décision/modèle, règles déclenchées, pays, volume horaire, historique d'une transaction, feature store, monitoring ML, latence et erreurs, index TTL | `fraud_alerts`, `transaction_logs`, `logs`, `ml_features` (mongo_writer.py), `ml_monitoring` (ml/monitor.py) |
| [`snowflake_olap.sql`](../queries/snowflake_olap.sql) | 5 requêtes étoile : région/trimestre, wallets vs cartes, pays à risque, GMV mois sur mois (LAG), Dynamic Table `dt_daily_revenue` | `fact_transactions` + `dim_*` (etl/load_snowflake.py) — **non exécuté**, dry-run |

---

## `terraform/` — Infrastructure as Code de la cible AWS

Validé (`make tf-validate`), jamais appliqué. Détail : [terraform/README.md](../terraform/README.md).

| Chemin | Rôle |
|---|---|
| [`bootstrap/main.tf`](../terraform/bootstrap/main.tf) | Bucket S3 versionné et chiffré du state Terraform |
| [`stack/main.tf`](../terraform/stack/main.tf) | Composition des modules, alarmes CloudWatch (CPU RDS, lag du scorer), SNS, budget mensuel |
| [`envs/dev/main.tf`](../terraform/envs/dev/main.tf), [`envs/prod/main.tf`](../terraform/envs/prod/main.tf) | Dimensionnement par environnement, backend S3, tags de coût |
| [`modules/network`](../terraform/modules/network/main.tf) | VPC 3 AZ, sous-réseaux public/app/data, NAT, endpoint S3, Flow Logs |
| [`modules/security`](../terraform/modules/security/main.tf) | CMK KMS, Secrets Manager, security groups par service, rôles IAM ECS |
| [`modules/rds`](../terraform/modules/rds/main.tf) | PostgreSQL 16 Multi-AZ, réplication logique (CDC), TLS forcé, réplica |
| [`modules/msk`](../terraform/modules/msk/main.tf) | Kafka 3 brokers, RF 3, IAM + TLS, création auto de topics désactivée |
| [`modules/elasticache`](../terraform/modules/elasticache/main.tf) | Redis 7 Multi-AZ chiffré (feature store vélocité) |
| [`modules/mongodb_atlas`](../terraform/modules/mongodb_atlas/main.tf) | Cluster Atlas 7, PrivateLink, utilisateur `stripe_app` en `readWrite` |
| [`modules/storage`](../terraform/modules/storage/main.tf) | Buckets data lake et DAGs, SSE-KMS, TLS obligatoire, cycle de vie |
| [`modules/compute`](../terraform/modules/compute/main.tf) | ECR + ECS Fargate ARM64 : scorer, mongo-writer, ml-monitor, dashboard |
| [`modules/airflow`](../terraform/modules/airflow/main.tf) | MWAA pour `dags/stripe_daily_etl.py` |

---

## Fichiers non-Python (schéma, config)

| Fichier | Contenu | Lien |
|---|---|---|
| `init/postgres/01_ddl.sql` | 6 tables, index, triggers, publication Debezium, vues matérialisées, rôles `replication_user`/`analytics_reader` | [→](../init/postgres/01_ddl.sql) |
| `init/postgres/02_rgpd.sql` | Fonction `anonymize_customer(uuid)` : droit à l'effacement sans suppression (client anonymisé, transactions détachées) | [→](../init/postgres/02_rgpd.sql) |
| `init/mongo/01_init_collections.js` | 7 collections, index, TTL RGPD | [→](../init/mongo/01_init_collections.js) |
| `init/mongo/02_app_user.js` | Utilisateur applicatif Mongo | [→](../init/mongo/02_app_user.js) |
| `config/debezium-connector.json` | Config connecteur CDC (template) | [→](../config/debezium-connector.json) |
| `config/debezium-connector-commentaire.md` | Le JSON ci-dessus expliqué ligne par ligne (JSON ne supporte pas les commentaires natifs) | [→](../config/debezium-connector-commentaire.md) |
| `docker-compose.yml` | 8 services par défaut + profils `flink`/`airflow` | [→](../docker-compose.yml) |
| `Makefile` | Tous les points d'entrée (`make up`, `make ml-train`, etc.) | [→](../Makefile) |

---

## Diagrammes associés

- [`presentation/stripe_architecture_globale.drawio`](../presentation/stripe_architecture_globale.drawio) — vue d'ensemble services/flux
- [`presentation/stripe_code_structure.drawio`](../presentation/stripe_code_structure.drawio) — ce document, en version visuelle (fichier → fonctions → imports)
- [`presentation/stripe_erd_oltp.drawio`](../presentation/stripe_erd_oltp.drawio) — schéma Postgres
- [`presentation/stripe_mongodb_structure.drawio`](../presentation/stripe_mongodb_structure.drawio) — schéma MongoDB
- [`presentation/stripe_aws_cible.drawio`](../presentation/stripe_aws_cible.drawio) — architecture physique de la cible AWS, fidèle à `terraform/envs/prod` (généré par script, régénéré à chaque évolution)
