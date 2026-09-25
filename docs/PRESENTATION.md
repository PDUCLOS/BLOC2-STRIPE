# Stripe Polyglot — Architecture de détection de fraude en temps réel

> Projet Bloc 2 — Certification Jedha Architecte en intelligence artificielle (RNCP41993)
> Démo end-to-end d'une plateforme de paiement polyglot : PostgreSQL · MongoDB · Kafka · Debezium · Redis · Flink · Streamlit · Airflow · XGBoost · MLflow · Evidently
>
> **Snowflake est branché depuis le 25/09/2026** sur un compte d'essai :
> schéma en étoile créé par `etl/snowflake_setup.py`, 59 119 transactions
> chargées par `etl/load_snowflake.py` (à la main puis par le DAG Airflow,
> MERGE idempotent vérifié : 0 doublon au rejeu), requêtes OLAP et Dynamic
> Table exécutées. Le premier export réel a révélé deux bugs invisibles en
> dry-run (`executemany` sur un `MERGE`, sous-requête corrélée dans un
> `MERGE`), corrigés le jour même. Sans compte (CI), l'export reste en dry-run.

---

## 1. Problématique métier

Construire une plateforme de paiement capable de **détecter les transactions frauduleuses en temps réel**, sans que l'OLTP qui sert les paiements ne soit ralenti par le scoring, et sans que les analyses OLAP bloquent les requêtes transactionnelles.

**Contraintes :**
- **Latence** : décision de fraude en < 1 seconde après l'INSERT
- **Scalabilité** : chaque couche peut monter en charge indépendamment
- **Traçabilité** : tous les événements sont logués pour audit et amélioration du modèle
- **Polyglot** : chaque technologie a sa raison d'être (pas de "tout dans une seule DB")
- **RGPD** : TTL automatique sur les logs, pseudonymisation des données client

---

## 2. Architecture — vue d'ensemble

```
┌──────────────────────────────────────────────────────────────────────┐
│                        COUCHE TRANSACTIONNELLE                        │
│                                                                       │
│   ┌─────────────┐    INSERT    ┌──────────────┐                      │
│   │  Producer   │─────────────▶│  PostgreSQL  │  OLTP — source de     │
│   │  (Python)   │              │     16       │  vérité (merchants,   │
│   └─────────────┘              │  (bookworm)  │  customers, txns)     │
│                                └──────┬───────┘                      │
│                                       │                              │
└───────────────────────────────────────┼──────────────────────────────┘
                                        │
                                        │ Logical replication
                                        │ (wal_level=logical, slot pgoutput)
                                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         COUCHE BUS ÉVÉNEMENTIEL                       │
│                                                                       │
│                              ┌─────────────────┐                     │
│                              │    Debezium     │  CDC — capture       │
│                              │   Connect 2.6   │  chaque changement   │
│                              └────────┬────────┘  de la DB et push   │
│                                       │           dans Kafka          │
│                                       ▼                              │
│                              ┌─────────────────┐                     │
│                              │   Kafka 7.6     │  KRaft, 12 partitions│
│                              │ (1 broker PoC)  │  retention 7-30j     │
│                              └────────┬────────┘                     │
│                                       │                              │
└───────────────────────────────────────┼──────────────────────────────┘
                                        │
                                        │ Stream subscription
                                        │ (consumer group)
                                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       COUCHE STREAM PROCESSING                       │
│                                                                       │
│                              ┌─────────────────┐                     │
│                              │   PyFlink 1.18  │  Job DataStream       │
│                              │ (1 JobManager   │  • Source : txns CDC│
│                              │  + 1 TaskMgr)   │  • Map : enrich Redis│
│                              └────────┬────────┘  • Map : score       │
│                                       │           • Filter : alertes  │
│                                       │           • Sink : Kafka      │
│                                       ▼                              │
│                              ┌─────────────────┐                     │
│                              │  Redis 7-alpine │  Feature store online│
│                              │                 │  • velocity 1h/24h    │
│                              │                 │  • features client    │
│                              │                 │  • TTL 1h (sliding)   │
│                              └─────────────────┘                     │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
                                        │
                                        │ Sink Kafka
                                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        COUCHE PERSISTENCE NoSQL                       │
│                                                                       │
│                              ┌─────────────────┐                     │
│                              │  Consumer Kafka │  Lit stripe.payments │
│                              │  → Mongo (Py)   │  .events et écrit    │
│                              └────────┬────────┘  dans MongoDB        │
│                                       ▼                              │
│                              ┌─────────────────┐                     │
│                              │   MongoDB 7     │  • transaction_logs  │
│                              │                 │    (TTL 90j RGPD)    │
│                              │                 │  • fraud_alerts      │
│                              │                 │  • ml_features       │
│                              │                 │  • customer_feedback │
│                              │                 │  • user_interactions │
│                              └─────────────────┘                     │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
                                        │
                                        │ Lecture live
                                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        COUCHE VISUALISATION                          │
│                                                                       │
│                              ┌─────────────────┐                     │
│                              │  Streamlit 1.37 │  Login + 2 onglets   │
│                              │  (Python 3.11)  │  • Vue d'ensemble    │
│                              │                 │  • Performance ML    │
│                              │                 │                      │
│                              │                 │                      │
│                              │                 │                      │
│                              └─────────────────┘                     │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                       COUCHE ANALYTIQUE (batch)                       │
│                                                                       │
│   ┌─────────────┐    ETL    ┌─────────────────┐                      │
│   │  Airflow    │──────────▶│   Snowflake     │  OLAP — star schema  │
│   │  standalone │ quotidien  │ (DWH compte     │  (dim_*, fact_*)     │
│   │  (profil)   │ 02:00 UTC  │  trial AWS)     │  pour reporting BI   │
│   └─────────────┘            └─────────────────┘                      │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                     COUCHE MACHINE LEARNING                          │
│                                                                       │
│   Redis (features live) + Postgres (historique + label vérité       │
│   terrain) ──▶ ml/train_fraud_model.py (XGBoost) ──▶ MLflow          │
│   (tracking + registre de modèles) ──▶ ml/models/*.pkl               │
│   ──▶ chargé par le scorer si SCORING_ENGINE=ml (fallback rules      │
│   automatique si pas encore entraîné)                                │
│                                                                       │
│   ml-monitor (Evidently, boucle continue) : compare la fenêtre       │
│   courante à la référence d'entraînement (drift + recall live)       │
│   ──▶ déclenche automatiquement un réentraînement si dérive          │
│   ──▶ écrit dans MongoDB.ml_monitoring, lu par l'onglet              │
│   "Performance ML" du dashboard                                      │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 3. Justification des choix techniques (le "pourquoi")

### 3.1 PostgreSQL 16 — l'OLTP, source de vérité transactionnelle

**Pourquoi pas une seule base pour tout ?**
Le pattern "database per concern" (polyglot persistence) garantit que la latence d'une requête OLAP ne dégrade jamais un INSERT transactionnel. Si le data scientist lance un `SELECT COUNT(*) ... GROUP BY` à 3h du matin, l'API paiement reste fluide.

**Pourquoi PostgreSQL spécifiquement ?**
- ACID strict (les paiements ne peuvent pas être "à peu près validés")
- `wal_level=logical` activé : permet la réplication logique pour le CDC
- JSONB pour les métadonnées flexibles de transaction (device fingerprint, session_id…)
- Mature, opérationnel, utilisé par Stripe, Notion, GitLab…

**Choix d'implémentation concrets :**
- UUID v4 sur toutes les PK (sécurité par non-énumération, distribution)
- Index composite `(merchant_id, created_at DESC)` : la requête la plus fréquente est "dernières transactions d'un merchant"
- Index partiel `WHERE fraud_score IS NOT NULL` : ne scanne que les transactions déjà scorées
- Trigger `updated_at` automatique
- Vues matérialisées `mv_daily_revenue` et `mv_merchant_stats` pour le reporting rapide
- **Publication Debezium ciblée** : seulement 3 tables sur 6 (transactions, refunds, fraud_indicators). Pas de bruit inutile dans le bus d'événements.
- **Rôle `replication_user` dédié** : Debezium n'utilise jamais le `stripe_app`, séparation claire des privilèges.

---

### 3.2 Apache Kafka 7.6 — le bus événementiel

**Pourquoi un bus ?**
Sans Kafka, le système serait synchrone : à chaque INSERT Postgres, le scoring fraud tournerait dans la même transaction, ce qui bloquerait l'API paiement. Avec Kafka, l'INSERT retourne instantanément, le scoring se fait en arrière-plan, et chaque consommateur (scoring, audit, data lake) peut rejouer l'historique à son rythme.

**Pourquoi KRaft (sans Zookeeper) ?**
KRaft est le mode consensus natif de Kafka depuis 3.3, stabilisé en 3.6+. Il élimine la dépendance externe à Zookeeper, simplifie l'opération, et réduit la latence de quelques ms. C'est désormais le défaut recommandé par Confluent.

**Choix d'implémentation :**
- **12 partitions** sur `stripe.payments.events` : permet de paralléliser le scoring jusqu'à 12 consommateurs (scalabilité horizontale)
- **Rétention 7-30 jours** selon le topic : assez pour rejouer un bug ou backfiller un dashboard
- **Compression snappy** sur le producer : ~70% de gain sur des payloads JSON
- **Dual listener** (`kafka:9092` interne + `localhost:29092` host) : pour que les scripts Python sur la machine et les conteneurs Docker accèdent au même broker avec la bonne résolution DNS

**Topics applicatifs :**
- `stripe.public.transactions` (CDC, 1 partition, 7j) — créé par Debezium
- `stripe.public.refunds` (CDC, 1 partition, 7j)
- `stripe.public.fraud_indicators` (CDC, 1 partition, 7j)
- `stripe.payments.events` (12 partitions, 30j) — toutes les transactions scorées
- `stripe.fraud.alerts` (3 partitions, 30j) — seulement les décisions `review`/`block`
- `stripe.etl.dead-letter` (3 partitions, ∞) — les messages non parsables ou en erreur

---

### 3.3 Debezium 2.6 — le Change Data Capture

**Pourquoi pas un trigger "AFTER INSERT" qui push dans Kafka ?**
Deux problèmes : (1) c'est couplé à la DB, donc chaque fois qu'on touche au schéma on risque de casser, (2) ça ne capture que les INSERT, pas les UPDATE/DELETE. Or dans la vraie vie, une transaction peut être **mise à jour** quand on calcule le `fraud_score` a posteriori.

**Pourquoi pas un polling maison toutes les N secondes ?**
Coûteux en DB load, et il y a toujours un décalage entre le commit et la lecture. La réplication logique PostgreSQL stream les changements en temps réel (sub-seconde) avec une garantie d'ordre par transaction.

**Comment ça marche techniquement :**
1. PostgreSQL écrit chaque changement dans le **Write-Ahead Log** (WAL) avec un format spécial `pgoutput`
2. Debezium ouvre une **connexion de réplication** dédiée (pas une connexion client normale), et lit le WAL en streaming
3. Debezium sérialise chaque changement en JSON, et `produce` dans Kafka
4. Le **slot de réplication** (`stripe_debezium_slot`) garantit que Debezium ne "saute" aucun changement, même s'il est déconnecté temporairement

**Point critique rencontré en démo :** avec `snapshot.mode: initial` et des tables **vides** au moment du deploy, Debezium reste bloqué en mode "No previous offsets found" et ne stream jamais. Fix : `snapshot.mode: no_data` (démarre en streaming à partir du LSN courant, sans instantané initial). Documenté pour ARM64 + Postgres 16.

**Transform `unwrap`** : Debezium encapsule chaque event dans une envelope `{"before": ..., "after": ...}`. Le transform `ExtractNewRecordState` extrait directement l'état final de la ligne, plus simple à consommer.

---

### 3.4 Apache Flink + Redis — le feature store + le scoring temps réel

**Pourquoi un feature store (Redis) ?**
Pour détecter la fraude, on a besoin de features qui dépendent de **l'historique récent** d'un client :
- "Combien de transactions ce client a-t-il fait dans la dernière heure ?" (velocity 1h)
- "Combien dans les dernières 24h ?" (velocity 24h)
- "Quel est son pays d'origine habituel ?"

Ces features ne sont pas dans la table `transactions` — elles sont **dérivées en temps réel** du flux d'événements. Redis est le bon outil : latence sub-ms, structures de données natives pour les fenêtres glissantes (sorted sets), et TTL automatique.

**Pourquoi Flink (et pas un simple consommateur Python) ?**
Pour une démo locale, un consommateur Python fait le job. Mais en prod avec 1000+ txns/s, Flink apporte :
- **Exactly-once** : pas de double-scoring si le consommateur crash
- **State management distribué** : si une fenêtre de velocity s'étend sur plusieurs partitions, Flink sait merger les états
- **Checkpointing** : reprise après crash en < 1 minute
- **Backpressure** : si Redis rame, Flink ralentit automatiquement le consommateur Kafka
- **Watermarks** : gestion correcte des events en retard (event time vs processing time)

**Architecture du job PyFlink :**
```
stripe.public.transactions (Kafka source)
   │
   ├── Map : score_transaction
   │      ├── Redis GET features:<customer_id>
   │      ├── Redis ZADD velocity:<customer_id>:1h  (sliding window)
   │      ├── Redis ZREMRANGEBYSCORE (purge >1h)
   │      └── Calcul score = 0.1 + Σ(règles)
   │
   ├── Filter decision in [review, block]
   │      │
   │      └── Kafka sink → stripe.fraud.alerts
   │
   └── Kafka sink → stripe.payments.events (toutes les txns scorées)
```

**Deux moteurs de scoring, sélectionnés par `SCORING_ENGINE` (env var) :**

`SCORING_ENGINE=rules` (défaut) applique 5 règles statiques ; `SCORING_ENGINE=ml`
charge le modèle XGBoost entraîné (`ml/train_fraud_model.py`, détaillé en
§3.8) et **retombe automatiquement sur les règles** si aucun modèle n'a
encore été entraîné (`ml/models/*.pkl` absent) — jamais de crash, juste un
score toujours moins bon en attendant le premier `make ml-train`.

**5 règles de scoring (moteur `rules`) :**

| Règle | Trigger | Poids |
|---|---|---|
| R1_high_amount | `amount > 1000€` | +0.35 |
| R2_card_testing | `0 < amount < 2€` ET `device ∈ {pos, mobile}` | +0.15 |
| R3_high_risk_geo | `ip_country ∈ {RU, NG, KP, IR, VE, BY}` | +0.40 |
| R4_velocity_1h | `velocity_1h > 10 txns/h` | +0.25 |
| R5_velocity_24h | `velocity_24h > 50 txns/24h` | +0.15 |

Chaque transaction scorée porte un champ `model_version` (`rule-based-v1` ou
`xgboost-v1`) qui trace quel moteur a produit la décision — utile pour
comparer les deux dans le dashboard (onglet "Performance ML") sans
ambiguïté sur qui a scoré quoi.

**Décision (identique quel que soit le moteur) :**
- `score ≥ 0.85` → **block** (refuse la transaction à la source, intégration future avec PSP)
- `score ∈ [0.6, 0.85[` → **review** (alerte au risk analyst, la transaction passe)
- `score < 0.6` → **allow**

**Pattern Redis utilisé — sliding windows avec sorted sets :**
```
ZADD velocity:cust_id:1h  <timestamp>  <timestamp>
ZREMRANGEBYSCORE velocity:cust_id:1h  0  (now - 3600)
EXPIRE velocity:cust_id:1h  3700  (pour cleanup auto)

ZCARD velocity:cust_id:1h  → donne le count dans la dernière heure
```

**Point technique rencontré :** l'image Docker PyFlink custom sur Mac M-series (ARM64) est **impossible à builder** : cascade de bugs (numpy 1.21.4 imposé par setup.py, JDK headers manquants, `ClassCastException: [B` au runtime). **Repli démo** : un job Python "Flink-like" (`producers/flink_like_job.py`) qui implémente exactement la même logique DataStream. Le code est documenté pour expliquer que c'est un repli démo, pas une limitation de l'archi. En prod, on déploie un vrai job PyFlink via Flink standalone ou KDA.

---

### 3.5 MongoDB 7 — la persistance NoSQL pour les logs et features ML

**Pourquoi pas tout dans Postgres ?**
Trois raisons :
1. **Volume** : les logs de transaction peuvent atteindre 10-100x le volume OLTP. Remplir Postgres avec ça plomberait les performances transactionnelles.
2. **Schéma flexible** : `transaction_logs.payload` contient le JSON complet avec les champs évolutifs (scoring, model_version, etc.). MongoDB gère ça nativement sans migration.
3. **Requêtes analytiques riches** : recherche full-text sur les user interactions, agrégations sur les alertes par pays, par merchant, par fenêtre de temps. MongoDB est plus rapide que Postgres pour ce profil.

**Collections :**

| Collection | Usage | TTL | Index clés |
|---|---|---|---|
| `transaction_logs` | Log append-only de chaque event (scoring, alert) | 90 jours (RGPD) | `txn_id`, `event_type + created_at` |
| `user_interactions` | Clics, échecs d'auth, sessions | 30 jours (RGPD) | `customer_id + timestamp` |
| `ml_features` | Instantané consolidé des features (pour entraînement batch) | infini | `customer_id` (unique) |
| `customer_feedback` | Disputes, contestations, notes | infini | `customer_id`, `merchant_id` |
| `fraud_alerts` | Alertes émises par Flink | infini | `txn_id`, `decision + created_at` |

**Choix d'implémentation :**
- **TTL automatique** : `expireAfterSeconds: 7776000` (90j) sur `transaction_logs.created_at`. MongoDB purge automatiquement. C'est de la conformité RGPD par construction, pas un script de nettoyage à maintenir.
- **Utilisateur applicatif séparé** (`stripe_app`) avec droits `readWrite` uniquement sur `stripe_nosql`. Pas de `root` dans le code applicatif.

---

### 3.6 Streamlit — le dashboard temps réel

**Pourquoi Streamlit (et pas Grafana ou un front React) ?**
Pour une démo, Streamlit est imbattable : Python pur, auto-refresh, `st.metric()` pour les KPIs, intégration native Plotly. Pour de la prod BI, on passerait à Grafana (avec source Kafka/Postgres/Redis) ou à un front React.

**Accès protégé par login** (hash SHA-256 du mot de passe dans `.env`, anti-bruteforce) — connexion de démo sur http://localhost:8501 : **`admin` / `Bloc2-Demo-2026`** — puis **2 onglets** (`dashboard/app.py`) :

| Onglet | Données (fonction → source) | Cas d'usage |
|---|---|---|
| **Vue d'ensemble** | `kpis_from_pg()`, `txn_over_time()`, `fraud_by_country()`, `top_merchants()`, `recent_suspicious()` → PostgreSQL (rôle `analytics_reader`) ; `fraud_alerts_mongo()` → MongoDB `fraud_alerts` ; `redis_stats()` → Redis | Vue executive + travail du risk analyst |
| **Performance ML** | `ml_monitoring_latest()` / `ml_monitoring_history()` → MongoDB `ml_monitoring` ; `fraud_score_by_model_version()` → PostgreSQL | Dérive, précision/rappel servis, comparaison `rule-based-v1` vs `xgboost-v1` |

**Auto-refresh modéré** (5s) : on évite de marteler la DB tout en gardant l'illusion du temps réel. Sur la vue d'ensemble, on pourrait utiliser `st.fragment` pour ne refetcher que les KPIs sans rerender toute la page.

---

### 3.7 Snowflake — l'OLAP pour le reporting batch

**Pourquoi un OLAP séparé (et pas de requêtes directes sur Postgres) ?**
Les requêtes OLAP sont **scans massifs** sur des millions de lignes, incompatibles avec la charge transactionnelle. Snowflake (ou BigQuery, Redshift) est optimisé pour : `SELECT SUM(amount) GROUP BY month, country, merchant_tier` sur 1 milliard de lignes.

**Pourquoi Snowflake spécifiquement ?**
- **Compute-storage separation** : on peut scale le warehouse (compute) sans toucher au stockage
- **Time Travel** : on peut `SELECT` l'état de la table à un point dans le passé
- **Zero-copy cloning** : créer un dev env sans dupliquer les données
- **Semi-structured data** (VARIANT) : nativement, on peut loader du JSON sans ETL préalable

**Star schema implémenté :**

```
                dim_date
                   │
                   │ date_key
                   ▼
fact_transactions ──────── dim_merchants
   │     │     │              │ merchant_key
   │     │     │              ▼
   │     │     └──────► dim_customers
   │     │                  │ customer_key
   │     │
   │     └────────────► dim_payment_methods
   │                       │ pm_key
   │
   └──────────────────► dim_geography
                           │ geo_key
```

**Clustering** : `CLUSTER BY (date_key, merchant_key)` sur la fact table. Les requêtes "par jour et par merchant" sont ultra-rapides, c'est exactement le pattern d'usage BI.

**Stratégie de chargement : `MERGE INTO`** :
- Pour les dimensions (merchants, customers) : upsert (les nouvelles lignes s'ajoutent, les existantes se mettent à jour)
- Pour la fact : insert-only (idempotent grâce à `txn_id UNIQUE`)

**ETL déclenché par** : le DAG Airflow (`dags/stripe_daily_etl.py`, cf. §3.8), planifié 02:00 UTC. Reste manuellement lançable via `make snowflake-export` même sans Airflow démarré (le DAG appelle exactement le même script, il ne fait qu'orchestrer/planifier).

---

### 3.8 MLflow + Evidently — entraînement, tracking et monitoring du modèle ML

**Le problème que ça résout :** un modèle entraîné une fois et jamais revisité se dégrade avec le temps (les patterns de fraude évoluent, la distribution du trafic change). Il faut (1) savoir comparer les versions successives du modèle, et (2) détecter automatiquement quand le modèle en production décroche, plutôt que de s'en apercevoir a posteriori sur un incident.

**MLflow — suivi + registre de modèles :**
- `ml/train_fraud_model.py` ouvre un run MLflow à chaque entraînement (manuel `make ml-train`, ou automatique via ml-monitor) : hyperparamètres, métriques (precision/recall/f1/ROC AUC), et le modèle sérialisé sont tous tracés ensemble
- Chaque run **enregistre une nouvelle version** du modèle `fraud-detector` dans le Registre de modèles — contrairement au fichier `.pkl` local (qui écrase la version précédente), l'historique complet reste consultable dans l'UI (`http://localhost:5001`)
- Backend SQLite + artefacts servis via l'API HTTP du serveur (`--serve-artifacts`) : un client MLflow (host macOS ou conteneur) n'a jamais besoin d'accéder directement au système de fichiers du conteneur `mlflow`, tout passe par HTTP

**Evidently — détection de dérive, dans `ml/monitor.py` (service `ml-monitor`) :**
- Boucle continue (toutes les `ML_MONITOR_INTERVAL_SECONDS`, 120s par défaut) : compare les features de la fenêtre courante (dernières `ML_MONITOR_WINDOW_MINUTES` minutes de transactions) à celles utilisées à l'entraînement (`DataDriftPreset`, `share_of_drifted_columns` sur les 5 features comportementales ; heure et jour exclus car ils dérivent par construction)
- Calcule en parallèle le rappel du modèle actuel sur les données fraîches (vérité terrain = `is_fraud_pattern` du générateur) — un modèle peut ne montrer aucun dérive de features et quand même décrocher en performance (dérive de concept), d'où les deux signaux, pas un seul
- Si `drift_share > ML_DRIFT_THRESHOLD` (0.3 par défaut) OU `recall < ML_MIN_RECALL` (0.7 par défaut) : déclenche automatiquement `ml/train_fraud_model.py` — un délai de carence (3× l'intervalle) évite de relancer un entraînement à chaque cycle tant que le problème persiste
- Chaque cycle écrit un document dans `MongoDB.ml_monitoring` (statut, dérive, performance, décision de réentraînement) — c'est ce que lit l'onglet "Performance ML" du dashboard

**Rechargement à chaud :** `ml/scoring.py::load_model()` revérifie le `mtime` du `.pkl` et recharge le modèle s'il a été réécrit : un réentraînement déclenché par `ml-monitor` est pris en compte par le scorer déjà lancé, sans redémarrage. Le coût est un `stat()` du fichier par appel, négligeable devant l'inférence.

**Pourquoi deux ports non-standards (5001, 8090) ?** MLflow écoute nativement sur 5000 et Airflow sur 8080, mais ces deux ports sont déjà pris par d'autres process sur une machine de dev typique (AirPlay Receiver macOS pour 5000, un autre stack Airflow local pour 8080 dans notre cas) — remappés côté host uniquement, les conteneurs communiquent toujours en interne sur leurs ports standards.

---

## 4. Décisions techniques non-évidentes (le "pourquoi" du détail)

### 4.1 Double listener Kafka (9092 + 29092)

Un seul listener `localhost:9092` ne marche pas :
- Depuis l'host (Mac) : `localhost:9092` fonctionne (port mapping Docker)
- Depuis un conteneur (Debezium, Flink) : `localhost:9092` = lui-même, ne fonctionne pas

Solution : 2 listeners :
- `PLAINTEXT://0.0.0.0:9092` → advertised `kafka:9092` (interne)
- `PLAINTEXT_HOST://0.0.0.0:29092` → advertised `localhost:29092` (host)

Le broker dit à chaque client "viens me voir à cette adresse" selon le listener utilisé.

### 4.2 Named volumes au lieu de bind mounts (Docker Desktop macOS)

Sur Mac M-series, les bind mounts (`./data/kafka:/var/lib/kafka/data`) passent par **virtiofs**, et `appuser (uid=1000)` du conteneur Kafka n'arrive pas à écrire dedans même avec `chmod 777` côté host. Le préflight `dub path /var/lib/kafka/data writable` plante en boucle.

**Solution** : named volumes (`kafka-data:/var/lib/kafka/data`) — gérés nativement par Docker, contournent virtiofs.

### 4.3 Image `postgres:16-bookworm` (Debian) au lieu d'`alpine`

L'image `postgres:16-alpine` n'a pas le binaire `locale` compilé. `pg_import_system_collations` plante au premier démarrage. Sur Debian (`bookworm`), les locales sont OK. Fix : `LANG=C.UTF-8` + `LC_ALL=C.UTF-8` + `POSTGRES_INITDB_ARGS="--locale=C.UTF-8"`.

### 4.4 Debezium `snapshot.mode: no_data` (pas `initial`)

Sur des tables vides au moment du deploy + Postgres 16, `snapshot.mode: initial` fait que Debezium reste bloqué en loop "No previous offsets found" et ne stream jamais. Avec `no_data`, on démarre direct en streaming à partir du LSN courant. Le instantané initial est utile seulement pour backfiller un data lake à partir de l'existant.

### 4.5 Write-back du `fraud_score` dans Postgres

Le scoring vit dans Flink, mais on **referme la boucle** en écrivant le score dans la table `transactions` d'origine. Conséquences :
1. Le dashboard Streamlit a accès au score en SQL direct (pas besoin de JOIN Kafka)
2. L'ETL Snowflake a la donnée dans la même source que les autres champs
3. **Boucle à surveiller** : Debezium capture cet UPDATE, le repousse dans Kafka, Flink le re-traite. Solution : la condition `WHERE fraud_score IS NULL` dans l'UPDATE garantit l'idempotence (deuxième passage : 0 row updated).

### 4.6 Dead-letter queue (DLQ)

Tous les messages qui ne peuvent pas être parsés ou scorés sont pushés dans `stripe.etl.dead-letter` avec contexte (topic, partition, offset, erreur, raw value tronqué). Plutôt que de silencieusement `continue` (perte d'info) ou de crash la boucle (perte de débit), on garde la trace et le pipeline continue. Pattern standard en event-driven.

### 4.7 Chargement du `.env` avec un helper `_env.py` à la racine

Chaque script (`producers/transaction_producer.py`, `producers/flink_like_job.py`, `dashboard/app.py`, etc.) commence par :
```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401
```

`_env.py` est à la racine du projet, donc trouvable depuis n'importe quel sous-dossier. Il utilise `python-dotenv` si dispo, sinon fait du parsing manuel (gère quotes, commentaires). Plus fiable que 8 versions de la même boucle `for line in env_path.read_text().splitlines()` dispersées dans le code.

### 4.8 Profils Docker Compose `flink` et `airflow` (optionnels)

Les services `flink-jobmanager`/`flink-taskmanager` (`profiles: ["flink"]`) et
`airflow` (`profiles: ["airflow"]`) ne démarrent pas par défaut avec `make up`
— on économise leur empreinte mémoire (~2 Go pour Flink, le mode standalone
Airflow n'est pas léger non plus) tant qu'on n'en a pas besoin. Activation :
`docker compose --profile flink up -d` ou `docker compose --profile airflow up -d`.
MLflow et ml-monitor, eux, sont dans le stack par défaut : plus légers, et
au cœur du scoring temps réel dès que `SCORING_ENGINE=ml`.

### 4.9 mongo/redis en volumes nommés (pas seulement Kafka, cf. §4.2)

§4.2 documentait déjà ce fix pour Kafka. Le même problème est réapparu sur
`mongo` et `redis` : leurs bind mounts (`./data/mongo`, `./data/redis`)
pointaient vers un dossier du dépôt qui — sur ce poste — vit sur un montage
Google Drive (FUSE), lequel ne supporte pas les verrous fichier POSIX que
WiredTiger (moteur de stockage MongoDB) exige au démarrage
(`Operation not permitted` sur `WiredTiger.wt`). Passés en volumes nommés
(`mongo-data`, `redis-data`), gérés par Docker, le problème disparaît — même
remède que Kafka, cause différente (Drive plutôt que virtiofs macOS).

### 4.10 MLflow : artefacts servis en proxy HTTP, pas en accès disque direct

`mlflow server --default-artifact-root /mlflow/artifacts` (sans
`--serve-artifacts`) suppose que **le client** a aussi accès à ce chemin —
vrai seulement si le client tourne dans le même conteneur. Un client externe
(le script d'entraînement lancé depuis le host, ou depuis le conteneur
`ml-monitor`) plante avec `Read-only file system: /mlflow` en essayant
d'écrire sur un chemin qui n'existe que dans le conteneur `mlflow`. Fix :
`--artifacts-destination` + `--serve-artifacts`, qui fait transiter les
artefacts par l'API HTTP du serveur (URIs `mlflow-artifacts:/...`) —
transparent pour n'importe quel client, où qu'il tourne.

### 4.11 `PYTHONUNBUFFERED=1` sur le conteneur ml-monitor

Sans cette variable, les `print()` de `ml/monitor.py` restaient coincés dans
le buffer stdout de Python (non-tty en conteneur = buffering complet, pas
ligne par ligne) — `docker logs` semblait montrer un service figé alors
qu'il tournait et écrivait bien dans MongoDB. Symptôme classique de
conteneur Python silencieux ; le fix est dans le Dockerfile (`ENV
PYTHONUNBUFFERED=1`), pas dans le code applicatif.

### 4.12 DAG Airflow : `snowflake_setup` volontairement absent du planning quotidien

Premier réflexe : chaîner `snowflake_setup >> snowflake_export` dans le DAG.
Mauvaise idée — `snowflake_setup.py` est un bootstrap **ponctuel** (créer le
schéma une fois), pas une étape à rejouer chaque nuit, et il échoue
bruyamment (`sys.exit(1)`) sans compte Snowflake réel configuré — ce qui est
le comportement voulu pour un lancement manuel (`make snowflake-setup`),
mais ferait échouer le DAG *tous les jours* sur une stack sans compte. Le DAG
n'orchestre que `snowflake_export` (qui, lui, retombe en dry-run sans
`SNOWFLAKE_ACCOUNT`) ; la création du schéma reste une commande manuelle séparée.
Le DAG passe `--date {{ ds }}` : pour une planification quotidienne à 02:00 UTC,
la date logique est la veille — sans cet argument, l'export prenait
`date.today()`, soit quelques minutes du jour qui commence.

---

## 5. Flux de données end-to-end (le scénario)

### 5.1 Transaction légitime

```
1. T+0ms     Producer INSERT 9900€ FR mobile   →  Postgres
2. T+50ms    Debezium lit le WAL               →  Kafka topic stripe.public.transactions
3. T+200ms   Flink consomme, score = 0.1       →  Pas d'alerte
4. T+201ms   Redis ZADD velocity:1h            →  count=1
5. T+202ms   Flink push l'event                →  Kafka stripe.payments.events
6. T+250ms   Mongo writer consomme             →  INSERT dans transaction_logs
7. T+260ms   Streamlit auto-refresh            →  Transaction visible dans "Live Transactions"
8. T+500ms   UPDATE fraud_score=0.1 dans Postgres (write-back)
9. T+2s      Flink push l'UPDATE               →  Kafka stripe.public.transactions (boucle!)
10. T+2.5s   Flink re-consomme mais WHERE fraud_score IS NULL → 0 row updated
```

### 5.2 Transaction frauduleuse (1500€ depuis la Russie)

```
1. T+0ms     Producer INSERT 150000 RUB pos    →  Postgres
2. T+50ms    Debezium CDC                       →  Kafka
3. T+200ms   Flink consomme
             ├─ R1_high_amount (>1000€)         →  +0.35
             ├─ R3_high_risk_geo (RU)            →  +0.40
             ├─ R4_velocity_1h (>10)             →  +0.25
             └─ score = 1.0 (clampé)              →  decision = BLOCK
4. T+201ms   Redis ZADD + features update
5. T+202ms   Kafka sink stripe.payments.events
6. T+203ms   Kafka sink stripe.fraud.alerts     ← topic dédié aux alertes
7. T+250ms   Mongo writer:
             ├─ INSERT transaction_logs
             └─ INSERT fraud_alerts (decision=block)
8. T+500ms   UPDATE fraud_score=1.0 dans Postgres
9. T+5s      Streamlit refresh:
             ├─ "Blocked: +1" dans Overview
             └─ Transaction rouge dans "Live Transactions"
             └─ Alerte dans "Fraud Alerts"
```

Latence totale INSERT → décision visible dans le dashboard : **< 1 seconde**

---

## 6. Conformité RGPD (ce qu'on a mis en place)

| Mécanisme | Implémentation |
|---|---|
| **Droit à l'effacement** (Art. 17) | `anonymize_customer(p_customer_id UUID)` PL/pgSQL ([`init/postgres/02_rgpd.sql`](../init/postgres/02_rgpd.sql), démontrée sous ROLLBACK dans [`queries/postgres_oltp.sql`](../queries/postgres_oltp.sql) §9) : remplace email par `anonymized_<8chars>@deleted.invalid`, set `name='ANONYMIZED'`, set `fingerprint='REDACTED'`, détache `customer_id` des transactions existantes |
| **Limitation de durée** (Art. 5(1)(e)) | TTL MongoDB : 90j sur `transaction_logs`, 30j sur `user_interactions`. Pas de script cron à maintenir. |
| **Pseudonymisation** (Art. 4(5)) | `payment_methods.fingerprint` = hash SHA-256, jamais le PAN. `metadata` ne stocke pas de PII direct. |
| **Sécurité par défaut** (Art. 25) | `analytics_reader` role n'a pas accès à `fingerprint`. `stripe_app` n'a accès qu'à `readWrite` sur `stripe_nosql`, pas admin. |
| **Chiffrement en transit** (Art. 32) | `PGSSLMODE=verify-full` activable, MSK TLS, Redis ElastiCache TLS en prod. |

---

## 7. Tests et validation

### 7.1 Test end-to-end automatisé

`tests/test_e2e.py` valide les 5 étapes du pipeline :
1. Health checks (Postgres, Redis, Mongo)
2. Insertion d'une transaction frauduleuse (1500€ + pays à risque)
3. Attente propagation Redis features (max 30s)
4. Vérification Mongo (transaction_logs + fraud_alerts)
5. Vérification write-back Postgres (fraud_score = 1.0)

### 7.2 Smoke tests manuels

```bash
make smoke
#  Postgres: SELECT count(*) FROM merchants  →  200
#  Mongo: transaction_logs.countDocuments()   →  growing
#  Redis: PING                                →  PONG
#  Kafka: topics count                        →  6+
#  Debezium: state RUNNING                    →  ok
```

---

## 8. Limites assumées et axes d'amélioration

### 8.1 Ce qui était une limite et a été comblé depuis

Ce projet a évolué après la première version de ce document ; ces points
étaient listés comme limites et sont maintenant implémentés et testés en
conditions réelles (stack Docker complète) :

- ~~Modèle de scoring simple (5 règles statiques)~~ → **Modèle XGBoost réel**,
  entraîné sur la vérité terrain du générateur, activable via `SCORING_ENGINE=ml`
  avec repli automatique sur les règles (§3.4, §3.8)
- ~~Pas d'Airflow~~ → **DAG Airflow réel** (`dags/stripe_daily_etl.py`,
  profil optionnel `airflow`), testé de bout en bout (§3.8, §4.12)
- ~~Pas de tracking/monitoring du modèle~~ → **MLflow** (suivi + registre
  de modèles) + **Evidently** (dérive + performance live, réentraînement
  automatique) — service `ml-monitor` (§3.8)
- ~~`ml_features` (collection Mongo) documentée mais jamais écrite~~ →
  alimentée en continu par `mongo_writer.py`, feature store offline réel

### 8.2 Limites restantes de la démo

- **Pas de vrai cluster Flink** : job Python "Flink-like" qui imite la logique DataStream. Causé par les bugs de build PyFlink sur ARM64 (numpy 1.21.4, JDK headers, ClassCastException [B).
- **CI exécutée sur GitHub depuis le 16/09/2026** : les trois jobs (`lint`, `e2e`, `terraform`) sont verts — 16/16 tests de bout en bout, requêtes SQL et MongoDB, `terraform validate`. Les six premiers passages ont révélé autant de bugs invisibles en local (plateforme ARM/x86, nom du connecteur Debezium, utilisateur MongoDB, vues matérialisées, index TTL des journaux, seuils ML appliqués au moteur à règles) : tous corrigés.
- **Modèle écrasé à chaque réentraînement** : le rechargement à chaud prend bien la nouvelle version, mais l'ancienne n'est conservée que dans MLflow ; pas de retour arrière automatique si la nouvelle version est moins bonne (cf. §3.8).
- **Airflow en mode standalone** (SQLite, SequentialExecutor, un seul process) : suffisant pour démontrer l'orchestration, pas dimensionné pour un vrai débit de DAGs concurrents.
- **`analytics_reader`** limite l'accès en lecture à `payment_methods.fingerprint`, mais aucun rôle équivalent n'existe encore côté MongoDB (un seul utilisateur applicatif `stripe_app` avec `readWrite` complet).
- **Snowflake tourne sur un compte d'essai** avec le rôle `ACCOUNTADMIN` et un mot de passe : rôles `LOADER`/`ANALYST`, clé RSA et network policy restent à faire pour un usage réel (README, « Passer à un compte Snowflake payant »).

### 8.3 Améliorations prioritaires restantes

1. **Vrai Flink en prod** : déployer le job PyFlink via Flink standalone (pas Docker) ou AWS Kinesis Data Analytics
2. **Streaming Snowflake** : au lieu d'un ETL batch quotidien, Snowpipe pour ingérer en continu
3. **Alerting** : PagerDuty / Slack quand `decision = block` et montant > seuil, ou quand ml-monitor détecte une dérive
4. **A/B testing** : servir 2 versions du modèle en parallèle en comparant leurs `model_version` respectifs sur le même trafic (le mécanisme de traçage existe déjà, pas encore le routing différencié)
5. **Backfill** : rejouer l'historique sur un nouveau modèle pour mesurer l'amélioration avant bascule complète
6. **GDPR data subject access request** : endpoint API pour qu'un user demande toutes ses données
7. **Retour arrière automatique du modèle** : comparer la précision servie avant/après réentraînement et restaurer la version MLflow précédente si elle baisse
8. **RBAC MongoDB** équivalent à `analytics_reader` côté Postgres

---

## 9. Arborescence du projet

```
.
├── docker-compose.yml            # 8 services actifs par défaut (Postgres, Mongo, Kafka, Debezium,
│                                  #   Redis, Dashboard, MLflow, ml-monitor)
│                                  # + 3 services optionnels : profil "flink" (JobManager, TaskManager),
│                                  #   profil "airflow" (orchestration ETL Snowflake)
├── Makefile                      # orchestration (up/down/init/seed/producer/ml-train/test)
├── demo.sh                       # one-shot: démarre tout pour la vidéo
├── README.md                     # mode d'emploi complet
│
├── .env.example                  # template (commit-friendly)
├── .env                          # secrets générés (gitignored)
├── _env.py                       # helper .env loader partagé
│
├── init/
│   ├── postgres/01_ddl.sql      # schéma OLTP complet (6 tables, indexes, vues, trigger, publication,
│   │                             #   rôles replication_user + analytics_reader)
│   └── mongo/                   # collections + index + TTL + user app
│
├── scripts/
│   ├── init_env.sh              # génère .env avec secrets aléatoires
│   ├── create_topics.sh         # topics Kafka applicatifs
│   ├── postgres_init_roles.sh   # crée replication_user + analytics_reader
│   ├── deploy_debezium.sh       # POST le connector sur Kafka Connect
│
├── seed/
│   └── seed_data.py             # 200 merchants, 5000 customers, ~8000 payment methods
│
├── producers/
│   ├── transaction_producer.py  # INSERT continu (95 % légitimes, 5 % fraude : 3 profils dont « furtive »)
│   ├── flink_like_job.py        # scoring fraud (Kafka→Redis→score[règles|ML]→Kafka) + write-back PG
│   └── mongo_writer.py          # consumer Kafka→Mongo (transaction_logs, fraud_alerts, ml_features)
│
├── flink/
│   ├── Dockerfile               # image PyFlink custom (optionnelle, profil "flink")
│   ├── requirements.txt
│   └── fraud_scoring_job.py     # version PyFlink DataStream (référence prod)
│
├── ml/                           # NOUVEAU — entraînement, inférence, monitoring du modèle fraude
│   ├── features.py               # feature engineering partagée entraînement/inférence
│   ├── train_fraud_model.py      # entraîne XGBoost, trace le run dans MLflow (make ml-train)
│   ├── scoring.py                # charge le modèle et prédit, utilisé par flink_like_job.py
│   ├── monitor.py                # boucle Evidently (drift+perf) + réentraînement auto (service ml-monitor)
│   ├── Dockerfile                # image du service ml-monitor
│   └── models/                   # .pkl + .meta.json générés par make ml-train (gitignored)
│
├── dags/                         # NOUVEAU — orchestration Airflow
│   └── stripe_daily_etl.py       # DAG quotidien (02:00 UTC) : export Postgres → Snowflake
│
├── dashboard/
│   ├── app.py                   # Streamlit, 2 onglets : Vue d'ensemble + Performance ML
│   └── Dockerfile               # image du service `dashboard` (port 8501)
│
├── etl/
│   ├── snowflake_setup.py       # crée warehouse + schéma (bootstrap manuel, PAS dans le DAG quotidien)
│   └── load_snowflake.py        # MERGE upsert + INSERT fact (dry-run gracieux sans credentials)
│
├── tests/
│   ├── test_e2e.py              # test bout-en-bout (Postgres/Redis/Mongo/pipeline live)
│   └── test_ml_model.py         # tests du module ml/ (features, cycle modèle, fallback) — sans Docker
│
├── config/
│   └── debezium-connector.json  # template du connector CDC
│
└── docs/
    ├── ARCHITECTURE.md           # référence technique fichier par fichier
    ├── PRESENTATION.md           # ce document — narratif complet du projet
    ├── SECURITY_COMPLIANCE_PLAN.md    # sécurité, conformité, monitoring
    ├── ML_INTEGRATION_STRATEGY.md     # stratégie ML détaillée (feature store, déploiement, monitoring)
    ├── OLAP_SCHEMA_DESIGN.md          # star schema Snowflake, clustering, optimisation
    └── NOSQL_DATA_MODEL.md            # schéma MongoDB, relations, stratégie d'indexation
```

---

## 10. Pour la présentation orale (3 minutes)

### Accroche (15s)

> "Imaginez que vous êtes Stripe, et qu'un fraudeur russe essaie de passer 1500€ avec un compte neuf. Vous voulez bloquer la transaction en < 1 seconde, sans que l'API de paiement rame, sans que l'analyse batch dégrade l'OLTP, et avec une trace complète pour l'audit. Voici comment on fait."

### Architecture (30s)

> "On sépare les responsabilités. PostgreSQL pour les paiements en temps réel. Kafka comme bus d'événements. Debezium capture chaque changement. Flink score la fraude avec Redis comme feature store. MongoDB stocke les logs et les alertes. Snowflake fait l'OLAP en batch. Streamlit visualise. Chaque techno a sa raison d'être, c'est ça le polyglot persistence."

### Démonstration live (1min 30)

> 1. Producer injecte 5 transactions/seconde
> 2. `kafka-console-consumer` montre le topic CDC qui se remplit
> 3. Le dashboard Streamlit montre les KPIs qui bougent en live
> 4. Une transaction frauduleuse (1500€ depuis la Russie) est insérée → 1 seconde plus tard, alerte rouge dans "Fraud Alerts"
> 5. Le test E2E confirme tout vert

### Points techniques forts (30s)

> "Trois points clés à retenir :
> 1. **Le write-back** ferme la boucle : le score calculé par Flink est écrit dans la table `transactions` source, donc le dashboard SQL a tout.
> 2. **Le DLQ** : tous les messages malformés vont dans `stripe.etl.dead-letter` avec contexte, on perd jamais d'info.
> 3. **Le dual listener Kafka** : le broker dit à chaque client où le joindre selon qu'il est host (port 29092) ou conteneur (port 9092, DNS `kafka`).

### Limites assumées (15s)

> "Flink custom Docker ne build pas sur Mac M-series ARM64 — bug connu de numpy/JDK. Pour la démo, un job Python 'Flink-like' fait la même logique DataStream. En prod, Managed Flink. Le scoring tourne en XGBoost avec repli automatique sur les règles, mais les labels viennent du générateur synthétique : en production, il faudrait des chargebacks confirmés. Et l'infra cloud est écrite en Terraform et validée, jamais appliquée faute de compte."

---

## 11. Métriques de succès de la démo

| Métrique | Valeur | Comment la montrer |
|---|---|---|
| Latence INSERT → décision | < 1s | INSERT + chrono + voir l'alerte dans le dashboard |
| Throughput | 5 txns/s (démo) → 1000+ en prod | Compteur dans Flink-like : `[1500 txns, 45 alerts] rate=4.8/s` |
| Détection d'une fraude évidente | Le test E2E insère une transaction à 1 500 € depuis un pays à risque : score ≥ 0,85, `block` et ligne `fraud_indicators` vérifiés | `make test` |
| Performance **servie** du modèle (live) | Précision 0,88 · rappel 0,64 sur 90 minutes de trafic (25/09/2026, 07:40-09:10 UTC : 32 281 transactions, 3 003 VP, 407 FP, 1 680 FN, 1 758 en revue). Le rappel a baissé de 0,82 à 0,40 au fil des réentraînements automatiques : cause et correctif (retour arrière) dans `docs/MLOPS.md` §5.5 | `queries/postgres_oltp.sql` §3 (24 h glissantes), onglet "Performance ML" |
| Performance **offline** du modèle servi | Modèle du 25/09 07:38 UTC (registre MLflow v71) : au seuil de blocage 0,85, précision 0,89 et rappel 0,80 ; à 0,5, précision 0,76 et rappel 0,88 ; AUC 0,96 sur 2 519 transactions de test. Réécrit à chaque réentraînement accepté : lire `ml/models/fraud_xgboost-v1.meta.json` | `make ml-train`, MLflow http://localhost:5001 |
| Dérive détecté | 0% (stack fraîchement seedée) | Onglet "Performance ML" → carte "Dérive (part colonnes)" |
| Disponibilité | 5/5 services core healthy (+ MLflow, ml-monitor) | `make status` |
| Conformité RGPD | TTL 90j automatique | `db.transaction_logs.getIndexes()` → voir le `expireAfterSeconds: 7776000` |

---

## 12. Conclusion

Ce projet démontre qu'on peut construire une plateforme de paiement **fiable, scalable et traçable** en combinant des briques open-source éprouvées, chacune avec un rôle précis. Le pattern "OLTP pour les transactions, CDC pour le streaming, feature store pour le temps réel, NoSQL pour les logs, OLAP pour l'analyse" est exactement le pattern utilisé par Stripe, Adyen, et les grands PSPs.

L'aspect le plus important n'est pas la stack elle-même, mais la **séparation claire des préoccupations** : un INSERT dans Postgres ne dépend pas de Kafka qui ne dépend pas de Flink qui ne dépend pas de Mongo. Chaque composant peut être redimensionné, redémarré, ou remplacé sans casser les autres.
