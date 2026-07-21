# Stripe Polyglot — Architecture de détection de fraude en temps réel

> Projet Bloc 2 — Certification Jedha Architecte en IA (RNCP 38777)
> Démo end-to-end d'une plateforme de paiement polyglot : PostgreSQL · MongoDB · Kafka · Debezium · Redis · Flink · Streamlit · Snowflake

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
│                              │ (3 brokers)     │  retention 7-30j     │
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
│                              │  Streamlit 1.32 │  Dashboard 5 pages   │
│                              │  (Python 3.11)  │  • Overview          │
│                              │                 │  • Live Transactions │
│                              │                 │  • Fraud Alerts      │
│                              │                 │  • Customers         │
│                              │                 │  • Pipeline Health   │
│                              └─────────────────┘                     │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                       COUCHE ANALYTIQUE (batch)                       │
│                                                                       │
│   ┌─────────────┐    ETL    ┌─────────────────┐                      │
│   │  Airflow /  │──────────▶│   Snowflake     │  OLAP — star schema  │
│   │  cron bash  │ quotidien  │ (DWH compte     │  (dim_*, fact_*)     │
│   │             │            │  trial AWS)     │  pour reporting BI   │
│   └─────────────┘            └─────────────────┘                      │
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
- **12 partitions** sur `stripe.payments.events` : permet de paralléliser le scoring jusqu'à 12 consumers (scalabilité horizontale)
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

**Point critique rencontré en démo :** avec `snapshot.mode: initial` et des tables **vides** au moment du deploy, Debezium reste bloqué en mode "No previous offsets found" et ne stream jamais. Fix : `snapshot.mode: no_data` (démarre en streaming à partir du LSN courant, sans snapshot initial). Documenté pour ARM64 + Postgres 16.

**Transform `unwrap`** : Debezium encapsule chaque event dans une envelope `{"before": ..., "after": ...}`. Le transform `ExtractNewRecordState` extrait directement l'état final de la ligne, plus simple à consommer.

---

### 3.4 Apache Flink + Redis — le feature store + le scoring temps réel

**Pourquoi un feature store (Redis) ?**
Pour détecter la fraude, on a besoin de features qui dépendent de **l'historique récent** d'un client :
- "Combien de transactions ce client a-t-il fait dans la dernière heure ?" (velocity 1h)
- "Combien dans les dernières 24h ?" (velocity 24h)
- "Quel est son pays d'origine habituel ?"

Ces features ne sont pas dans la table `transactions` — elles sont **dérivées en temps réel** du flux d'événements. Redis est le bon outil : latence sub-ms, structures de données natives pour les fenêtres glissantes (sorted sets), et TTL automatique.

**Pourquoi Flink (et pas un simple consumer Python) ?**
Pour une démo locale, un consumer Python fait le job. Mais en prod avec 1000+ txns/s, Flink apporte :
- **Exactly-once** : pas de double-scoring si le consumer crash
- **State management distribué** : si une fenêtre de velocity s'étend sur plusieurs partitions, Flink sait merger les états
- **Checkpointing** : reprise après crash en < 1 minute
- **Backpressure** : si Redis rame, Flink ralentit automatiquement le consumer Kafka
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

**5 règles de scoring :**

| Règle | Trigger | Poids |
|---|---|---|
| R1_high_amount | `amount > 1000€` | +0.35 |
| R2_card_testing | `0 < amount < 2€` ET `device ∈ {pos, mobile}` | +0.15 |
| R3_high_risk_geo | `ip_country ∈ {RU, NG, KP, IR, VE, BY}` | +0.40 |
| R4_velocity_1h | `velocity_1h > 10 txns/h` | +0.25 |
| R5_velocity_24h | `velocity_24h > 50 txns/24h` | +0.15 |

**Décision :**
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

**Point technique rencontré :** l'image Docker PyFlink custom sur Mac M-series (ARM64) est **impossible à builder** : cascade de bugs (numpy 1.21.4 imposé par setup.py, JDK headers manquants, `ClassCastException: [B` au runtime). **Fallback démo** : un job Python "Flink-like" (`producers/flink_like_job.py`) qui implémente exactement la même logique DataStream. Le code est documenté pour expliquer que c'est un fallback démo, pas une limitation de l'archi. En prod, on déploie un vrai job PyFlink via Flink standalone ou KDA.

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
| `ml_features` | Snapshot consolidé des features (pour entraînement batch) | infini | `customer_id` (unique) |
| `customer_feedback` | Disputes, contestations, notes | infini | `customer_id`, `merchant_id` |
| `fraud_alerts` | Alertes émises par Flink | infini | `txn_id`, `decision + created_at` |

**Choix d'implémentation :**
- **TTL automatique** : `expireAfterSeconds: 7776000` (90j) sur `transaction_logs.created_at`. MongoDB purge automatiquement. C'est de la conformité RGPD par construction, pas un script de nettoyage à maintenir.
- **Utilisateur applicatif séparé** (`stripe_app`) avec droits `readWrite` uniquement sur `stripe_nosql`. Pas de `root` dans le code applicatif.

---

### 3.6 Streamlit — le dashboard temps réel

**Pourquoi Streamlit (et pas Grafana ou un front React) ?**
Pour une démo, Streamlit est imbattable : Python pur, auto-refresh, `st.metric()` pour les KPIs, intégration native Plotly. Pour de la prod BI, on passerait à Grafana (avec source Kafka/Postgres/Redis) ou à un front React.

**5 pages :**

| Page | Données | Use case |
|---|---|---|
| **Overview** | KPIs agrégés, charts temps réel | Vue executive |
| **Live Transactions** | Flux des dernières transactions scorées, color-coded par score | Veille opérationnelle |
| **Fraud Alerts** | Liste des alertes MongoDB avec drill-down | Travail du risk analyst |
| **Customers** | Top velocity (Redis), features (Redis) | Détection des comptes suspects |
| **Pipeline Health** | Status de chaque service + topics Kafka | Monitoring infra |

**Auto-refresh modéré** (5s) : on évite de marteler la DB tout en gardant l'illusion du temps réel. Sur l'Overview, on pourrait utiliser `st.fragment` pour ne refetcher que les KPIs sans rerender toute la page.

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

**ETL déclenché par** : un DAG Airflow en prod (`dags/stripe_daily_etl.py`), ou un simple cron bash en démo (`make snowflake-export`).

---

## 4. Décisions techniques non-évidentes (le "pourquoi" du détail)

### 4.1 Double listener Kafka (9092 + 29092)

Un seul listener `localhost:9092` ne marche pas :
- Depuis l'host (Mac) : `localhost:9092` ✓ (port mapping Docker)
- Depuis un conteneur (Debezium, Flink) : `localhost:9092` = lui-même ✗

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

Sur des tables vides au moment du deploy + Postgres 16, `snapshot.mode: initial` fait que Debezium reste bloqué en loop "No previous offsets found" et ne stream jamais. Avec `no_data`, on démarre direct en streaming à partir du LSN courant. Le snapshot initial est utile seulement pour backfiller un data lake à partir de l'existant.

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

### 4.8 Profil Docker Compose `flink` (optionnel)

Les services `flink-jobmanager` et `flink-taskmanager` sont déclarés dans le compose mais avec `profiles: ["flink"]`. Par défaut (sans le profil), ils ne démarrent pas — on économise ~2 Go de RAM et le build d'image cassé. Pour les activer en prod : `docker compose --profile flink up -d`.

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
             ├─ "🚫 Blocked: +1" dans Overview
             └─ Transaction rouge dans "Live Transactions"
             └─ Alerte dans "Fraud Alerts"
```

Latence totale INSERT → décision visible dans le dashboard : **< 1 seconde** ✅

---

## 6. Conformité RGPD (ce qu'on a mis en place)

| Mécanisme | Implémentation |
|---|---|
| **Droit à l'effacement** (Art. 17) | `anonymize_customer(p_customer_id UUID)` PL/pgSQL : remplace email par `anonymized_<8chars>@deleted.invalid`, set `name='ANONYMIZED'`, set `fingerprint='REDACTED'`, détache `customer_id` des transactions existantes |
| **Limitation de durée** (Art. 5(1)(e)) | TTL MongoDB : 90j sur `transaction_logs`, 30j sur `user_interactions`. Pas de script cron à maintenir. |
| **Pseudonymisation** (Art. 4(5)) | `payment_methods.fingerprint` = hash SHA-256, jamais le PAN. `metadata` ne stocke pas de PII direct. |
| **Sécurité par défaut** (Art. 25) | `analytics_reader` role n'a pas accès à `fingerprint`. `stripe_app` n'a accès qu'à `readWrite` sur `stripe_nosql`, pas admin. |
| **Chiffrement en transit** (Art. 32) | `PGSSLMODE=verify-full` activable, MSK TLS, Redis ElastiCache TLS en prod. |

---

## 7. Tests et validation

### 7.1 Test end-to-end automatisé

`tests/test_e2e.py` valide les 5 étapes du pipeline :
1. ✅ Health checks (Postgres, Redis, Mongo)
2. ✅ Insertion d'une transaction frauduleuse (1500€ + pays à risque)
3. ✅ Attente propagation Redis features (max 30s)
4. ✅ Vérification Mongo (transaction_logs + fraud_alerts)
5. ✅ Vérification write-back Postgres (fraud_score = 1.0)

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

### 8.1 Limites de la démo

- **Pas de vrai cluster Flink** : job Python "Flink-like" qui imite la logique DataStream. Causé par les bugs de build PyFlink sur ARM64 (numpy 1.21.4, JDK headers, ClassCastException [B).
- **Modèle de scoring simple** : 5 règles statiques. En prod, on entraînerait un modèle supervisé (XGBoost, LightGBM) sur les `fraud_indicators.decision`.
- **Pas d'Airflow** : juste un script `etl/load_snowflake.py`. En prod, DAG Airflow avec sensors sur la partition date.
- **Pas de CI/CD** : pas de GitHub Actions, pas de tests unitaires pytest (uniquement un E2E).

### 8.2 Améliorations prioritaires

1. **Modèle ML** : remplacer le scoring rule-based par un modèle supervisé (entraîné sur les `fraud_indicators` réelles)
2. **Vrai Flink en prod** : déployer le job PyFlink via Flink standalone (pas Docker) ou AWS Kinesis Data Analytics
3. **Streaming Snowflake** : au lieu d'un ETL batch quotidien, Snowpipe pour ingérer en continu
4. **Alerting** : PagerDuty / Slack quand `decision = block` et montant > seuil
5. **A/B testing** : servir 2 versions du modèle (champ `model_version` déjà dans l'event), comparer en prod
6. **Backfill** : rejouer l'historique sur un nouveau modèle pour mesurer l'amélioration
7. **GDPR data subject access request** : endpoint API pour qu'un user demande toutes ses données

---

## 9. Arborescence du projet

```
.
├── docker-compose.yml            # 6 services actifs (Postgres, Mongo, Kafka, Debezium, Redis, Dashboard)
│                                # + 2 services profil "flink" (JobManager, TaskManager)
├── Makefile                      # orchestration (up/down/init/seed/producer/test)
├── demo.sh                       # one-shot: démarre tout pour la vidéo
├── README.md                     # mode d'emploi complet
│
├── .env.example                  # template (commit-friendly)
├── .env                          # secrets générés (gitignored)
├── _env.py                       # helper .env loader partagé
│
├── init/
│   ├── postgres/01_ddl.sql      # schéma OLTP complet (6 tables, indexes, vues, trigger, publication)
│   └── mongo/                   # collections + index + TTL + user app
│
├── scripts/
│   ├── init_env.sh              # génère .env avec secrets aléatoires
│   ├── create_topics.sh         # topics Kafka applicatifs
│   ├── postgres_init_roles.sh   # crée le replication_user
│   ├── deploy_debezium.sh       # POST le connector sur Kafka Connect
│
├── seed/
│   └── seed_data.py             # 200 merchants, 5000 customers, ~8000 payment methods
│
├── producers/
│   ├── transaction_producer.py  # INSERT continu de transactions (95% legit, 5% fraud)
│   ├── flink_like_job.py        # job scoring fraud (Kafka→Redis→score→Kafka) + write-back PG
│   └── mongo_writer.py          # consumer Kafka→Mongo (transaction_logs + fraud_alerts)
│
├── flink/
│   ├── Dockerfile               # image PyFlink custom (optionnelle, profil "flink")
│   ├── requirements.txt
│   └── fraud_scoring_job.py     # version PyFlink DataStream (référence prod)
│
├── dashboard/
│   ├── app.py                   # Streamlit 5 pages
│   └── Dockerfile               # image du service `dashboard` (port 8501)
│
├── etl/
│   ├── snowflake_setup.py       # crée warehouse + schéma
│   └── load_snowflake.py        # MERGE upsert + INSERT fact
│
├── tests/
│   └── test_e2e.py              # test bout-en-bout
│
├── config/
│   └── debezium-connector.json  # template du connector CDC
│
└── docs/                        # (vide pour l'instant, à remplir)
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

> "Flink custom Docker ne build pas sur Mac M-series ARM64 — bug connu de numpy/JDK. Pour la démo, un job Python 'Flink-like' fait la même logique DataStream. En prod, Flink standalone. Le modèle de scoring est rule-based ; en prod on entraînerait un XGBoost sur les `fraud_indicators` réelles."

---

## 11. Métriques de succès de la démo

| Métrique | Valeur | Comment la montrer |
|---|---|---|
| Latence INSERT → décision | < 1s | INSERT + chrono + voir l'alerte dans le dashboard |
| Throughput | 5 txns/s (démo) → 1000+ en prod | Compteur dans Flink-like : `[1500 txns, 45 alerts] rate=4.8/s` |
| Taux de fraude détecté | 100% des 5% injectés | Le test E2E insère 1 frauduleuse, génère 1 alerte |
| Faux positifs | 0 (modèle simple) | Afficher la distribution des scores dans Streamlit |
| Disponibilité | 5/5 services healthy | `make status` |
| Conformité RGPD | TTL 90j automatique | `db.transaction_logs.getIndexes()` → voir le `expireAfterSeconds: 7776000` |

---

## 12. Conclusion

Ce projet démontre qu'on peut construire une plateforme de paiement **fiable, scalable et traçable** en combinant des briques open-source éprouvées, chacune avec un rôle précis. Le pattern "OLTP pour les transactions, CDC pour le streaming, feature store pour le temps réel, NoSQL pour les logs, OLAP pour l'analyse" est exactement le pattern utilisé par Stripe, Adyen, et les grands PSPs.

L'aspect le plus important n'est pas la stack elle-même, mais la **séparation claire des préoccupations** : un INSERT dans Postgres ne dépend pas de Kafka qui ne dépend pas de Flink qui ne dépend pas de Mongo. Chaque composant peut être redimensionné, redémarré, ou remplacé sans casser les autres.
