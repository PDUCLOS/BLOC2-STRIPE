// Génère presentation/stripe_architecture_bloc2.docx — document d'architecture
// aligné sur l'implémentation réelle du dépôt (état au 15/09/2026).
const fs = require("fs");
const { Packer } = require("docx");
const L = require("./lib");
const { h1, h2, h3, p, bullets, numbered, code, note, table, image, cover, makeDoc, toc } = L;

const PRES = process.argv[2];   // dossier presentation/
const OUT = process.argv[3];
const img = (f) => `${PRES}/${f}`;

const children = [
  ...cover(
    "Architecture de Données Unifiée",
    "OLTP · OLAP · NoSQL · Pipeline · Sécurité · Machine Learning · Cloud",
    "Document d’architecture aligné sur le code du dépôt — état au 15 septembre 2026",
    [
      ["Candidat", "Patrice Duclos"],
      ["Certification", "Architecte en intelligence artificielle — RNCP41993 — Bloc 2"],
      ["Contexte", "Stripe — FinTech — Business Case"],
      ["Dépôt", "github.com/PDUCLOS/BLOC2-STRIPE"],
      ["Soutenance", "5 min de présentation + 15 min de questions — octobre 2026"],
    ],
  ),

  ...toc(),

  // ─────────────────────────────────────────────────────────────────────────
  h1("0. Comment lire ce document"),
  p("Chaque section distingue ce qui **tourne réellement** dans le PoC local de ce qui est **conçu pour la cible**. Aucun composant n’est présenté comme branché s’il ne l’est pas."),
  ...table(["Statut", "Signification", "Composants concernés"], [
    ["Implémenté et testé", "Code exécuté sur la stack Docker, couvert par `make test`, `make queries-check` ou le notebook d’audit, et rejoué en CI", "PostgreSQL, Debezium, Kafka, scorer (règles + XGBoost), Redis, MongoDB, MLflow, Evidently, Airflow, dashboard sécurisé, requêtes SQL/NoSQL"],
    ["Écrit et validé, non appliqué", "Code présent dans le dépôt et vérifié automatiquement, jamais déployé faute de compte", "Terraform de la cible AWS (`terraform validate` en CI)"],
    ["Conçu, non connecté", "Schéma et code prêts, exécutés en dry-run", "Snowflake (OLAP) : aucun compte, chargement simulé"],
  ], [22, 43, 35]),
  note("Le PoC tourne sur un poste personnel avec Docker Compose, pour une raison de budget. La cible cloud reprend exactement la même architecture logique : seuls les composants changent (section 10).", "info"),

  // ─────────────────────────────────────────────────────────────────────────
  h1("1. Contexte et problématique"),
  p("Stripe traite des milliards de transactions par an pour des millions de marchands. Une base PostgreSQL bien normalisée suffit au démarrage ; à grande échelle, on lui demande trois choses incompatibles en même temps : encaisser des millions de paiements avec des garanties ACID, répondre à des requêtes analytiques lourdes sur tout l’historique, et stocker des données semi-structurées dont le format change sans cesse."),
  p("La réponse retenue est une **architecture polyglotte** : chaque système fait ce pour quoi il est conçu, et un bus d’événements (Kafka alimenté par CDC) les relie sans ralentir la base transactionnelle."),
  ...table(["Défi", "Impact", "Système concerné", "Réponse dans le projet"], [
    ["Volume transactionnel", "Millions de paiements/jour, latence < 50 ms", "OLTP", "PostgreSQL 16, clé d’idempotence, index ciblés"],
    ["Analytique complexe", "Requêtes multi-dimensions sur l’historique", "OLAP", "Schéma en étoile Snowflake (dry-run) + vues matérialisées PostgreSQL"],
    ["Données semi-structurées", "Logs, alertes, features ML", "NoSQL", "MongoDB 7, embedding/referencing, TTL"],
    ["Intégration temps réel + batch", "Pas de couplage fort entre bases", "Pipeline", "Debezium → Kafka ; DAG Airflow quotidien"],
    ["Conformité", "PCI-DSS v4.0, RGPD", "Tous", "Aucun PAN, rôles séparés, anonymisation, TTL"],
    ["Fraude en temps réel", "Décision pendant que le paiement attend", "ML + NoSQL", "XGBoost + règles, features Redis, write-back atomique"],
  ], [20, 26, 16, 38]),

  h2("Vue d’ensemble"),
  ...image(img("stripe_architecture_globale.png"), "Figure 1 — Architecture globale telle qu’implémentée (presentation/stripe_architecture_globale.drawio)"),

  // ─────────────────────────────────────────────────────────────────────────
  h1("2. Modèle OLTP — PostgreSQL 16"),
  p("La base transactionnelle est la source de vérité. Le schéma est en **troisième forme normale** : un marchand, un client ou un moyen de paiement n’existe qu’à un seul endroit, ce qui supprime les anomalies de mise à jour. Le DDL complet est dans `init/postgres/01_ddl.sql`, exécuté au premier démarrage du conteneur."),
  h2("2.1 Tables"),
  ...table(["Table", "Rôle", "Points de conception"], [
    ["`merchants`", "Marchands", "`email` unique, index partiel sur `status = 'active'` (tiré à chaque transaction générée)"],
    ["`customers`", "Clients", "Index sur `email` et `segment`"],
    ["`payment_methods`", "Moyens de paiement", "`fingerprint` pseudonymisé : jamais de numéro de carte (PAN)"],
    ["`transactions`", "Paiements", "`amount BIGINT` en centimes (pas de flottant), `idempotency_key UNIQUE`, `fraud_score` écrit par le scorer, `metadata JSONB`"],
    ["`refunds`", "Remboursements", "Référence `transactions`"],
    ["`fraud_indicators`", "Trace des décisions de fraude", "Une ligne par décision `review` ou `block`, écrite dans la même transaction que le score"],
  ], [20, 22, 58]),
  ...code(`CREATE TABLE transactions (
    txn_id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    merchant_id      UUID NOT NULL REFERENCES merchants(merchant_id),
    customer_id      UUID REFERENCES customers(customer_id),
    pm_id            UUID REFERENCES payment_methods(pm_id),
    amount           BIGINT NOT NULL,          -- centimes
    currency         CHAR(3) NOT NULL,
    status           VARCHAR(20) NOT NULL,
    fraud_score      NUMERIC(5,4),             -- écrit par le scorer
    idempotency_key  VARCHAR(128) UNIQUE,      -- déduplication
    device_type      VARCHAR(50),
    ip_country       CHAR(2),
    metadata         JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE fraud_indicators (
    fraud_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    txn_id          UUID NOT NULL REFERENCES transactions(txn_id),
    anomaly_score   NUMERIC(5,4) NOT NULL,
    rules_triggered TEXT[],
    model_version   VARCHAR(20),
    decision        VARCHAR(10) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`),
  ...image(img("stripe_erd_oltp.png"), "Figure 2 — Modèle entité-association OLTP (presentation/stripe_erd_oltp.drawio)"),

  h2("2.2 Index"),
  ...table(["Index", "Colonnes", "Requête servie"], [
    ["`idx_txn_merchant`", "`(merchant_id, created_at DESC)`", "Tableau de bord marchand, top marchands"],
    ["`idx_txn_customer`", "`(customer_id, created_at DESC)`", "Historique client, contrôle de vélocité (requête §9)"],
    ["`transactions_idempotency_key_key`", "`idempotency_key` (unique)", "Déduplication : `EXPLAIN` montre un Index Scan"],
    ["`idx_txn_fraud`", "`fraud_score` partiel (`IS NOT NULL`)", "Transactions scorées uniquement"],
    ["`idx_txn_status`, `idx_txn_created_at`", "`status`, `created_at DESC`", "Suivi opérationnel"],
    ["`idx_fraud_txn`, `idx_fraud_decision`", "`txn_id` ; `(decision, created_at DESC)`", "Jointure avec transactions, file de revue"],
  ], [32, 33, 35]),

  h2("2.3 ACID, idempotence et write-back"),
  bullets([
    "**Atomicité** : le scorer écrit `transactions.fraud_score` et, pour une décision `review` ou `block`, la ligne `fraud_indicators`, dans **une seule transaction**. Si l’insertion échoue, la mise à jour du score est annulée.",
    "**Idempotence** : la mise à jour ne s’applique que si `fraud_score IS NULL`, et l’indicateur n’est inséré que si cette mise à jour a modifié une ligne. Un message CDC rejoué, ou l’événement produit par la mise à jour elle-même, ne crée ni second score ni doublon. Mesuré le 15/09 : 691 indicateurs, 0 transaction bloquée sans trace, 0 doublon.",
    "**Déduplication à l’entrée** : `idempotency_key UNIQUE` refuse un paiement rejoué par le client.",
    "**Cohérence** : clés étrangères et trigger `updated_at`.",
  ]),
  h2("2.4 Vues matérialisées et haute disponibilité"),
  p("`mv_daily_revenue` (revenu par jour et par devise) et `mv_merchant_stats` (volume, GMV et score moyen par marchand) sont des **vues PostgreSQL**, créées vides puis rafraîchies par `etl/refresh_views.py` (tâche du DAG Airflow). Un index unique sur chacune autorise `REFRESH CONCURRENTLY` sans bloquer les lecteurs."),
  p("En local, l’instance est unique. En cible, RDS PostgreSQL **Multi-AZ** (standby synchrone, bascule automatique en une à deux minutes) et un **réplica de lecture** pour le dashboard, décrits dans `terraform/modules/rds`."),

  // ─────────────────────────────────────────────────────────────────────────
  h1("3. Modèle OLAP — Snowflake (schéma en étoile)"),
  note("**Statut : conçu, non connecté.** Aucun compte Snowflake n’est rattaché au projet. `etl/snowflake_setup.py` crée le schéma et `etl/load_snowflake.py` affiche ce qu’il chargerait (dry-run). Les étapes pour un compte payant sont dans le README.", "warn"),
  p("L’analytique répond à des questions du type « revenu et fraude par région et par trimestre ». Un schéma 3NF imposerait trop de jointures : on dénormalise volontairement autour d’une table de faits."),
  h2("3.1 Table de faits"),
  ...code(`CREATE TABLE IF NOT EXISTS fact_transactions (
    txn_key        NUMBER AUTOINCREMENT NOT NULL PRIMARY KEY,
    txn_id         VARCHAR(36) NOT NULL UNIQUE,
    merchant_key   NUMBER REFERENCES dim_merchant(merchant_key),
    customer_key   NUMBER REFERENCES dim_customer(customer_key),
    pm_key         NUMBER REFERENCES dim_payment_method(pm_key),
    date_key       NUMBER REFERENCES dim_date(date_key),
    geo_key        NUMBER REFERENCES dim_geography(geo_key),
    amount_eur     NUMBER(18,2) NOT NULL,
    fee_amount     NUMBER(18,2),
    currency       CHAR(3),
    status         VARCHAR(20),
    fraud_score    NUMBER(5,4),
    is_fraud       BOOLEAN,
    device_type    VARCHAR(50),
    processing_ms  NUMBER(8,2),
    created_at     TIMESTAMP_TZ
)
CLUSTER BY (date_key, merchant_key);`),
  h2("3.2 Dimensions"),
  ...table(["Dimension", "Champs clés", "Remarque"], [
    ["`dim_date`", "`date_key` (AAAAMMJJ), `full_date`, `year`, `quarter`, `month`, `week`, `is_weekend`", "Pré-peuplée 2020-2029"],
    ["`dim_merchant`", "`merchant_key`, `merchant_id`, `name`, `email`, `country`, `tier`, `status`", "`valid_from` / `is_current` préparent une SCD de type 2"],
    ["`dim_customer`", "`customer_key`, `customer_id`, `segment`, `country`", "Aucune donnée directement identifiante"],
    ["`dim_payment_method`", "`pm_key`, `type`, `brand`, `is_digital_wallet`", ""],
    ["`dim_geography`", "`geo_key`, `country_code`, `region`, `is_high_risk`", "Reprend la liste de pays à risque du scorer"],
  ], [22, 53, 25]),
  h2("3.3 Performance et pré-agrégats"),
  bullets([
    "**Clustering** `(date_key, merchant_key)` : Snowflake élague les micro-partitions hors de la plage de dates ou du marchand demandé. Il n’y a ni index B-tree ni partitionnement manuel.",
    "**Pré-agrégat** : Dynamic Table `dt_daily_revenue` (`TARGET_LAG = '1 hour'`), équivalent Snowflake de `mv_daily_revenue`, définie dans `queries/snowflake_olap.sql`.",
    "**Semi-additivité** : `fraud_score` ne se somme pas. Les requêtes le moyennent toujours au grain du `GROUP BY`, jamais en faisant la moyenne de moyennes.",
    "**Archivage** : faits de plus de deux ans en Parquet/Iceberg sur S3 ; le cycle de vie du bucket (IA à 90 jours, Glacier IR à 365 jours) est codé dans `terraform/modules/storage`.",
  ]),

  // ─────────────────────────────────────────────────────────────────────────
  h1("4. Modèle NoSQL — MongoDB 7"),
  p("MongoDB stocke ce que PostgreSQL gère mal : des documents lus d’un bloc, dont la forme évolue (liste de règles déclenchées, sous-documents de monitoring), et des données à durée de vie limitée. Chaque champ ci-dessous est réellement écrit par le code."),
  ...table(["Collection", "Écrite par", "Contenu", "Durée de vie"], [
    ["`transaction_logs`", "`producers/mongo_writer.py`", "Journal brut de chaque transaction scorée (payload complet)", "TTL 90 jours"],
    ["`fraud_alerts`", "`mongo_writer.py`", "Décisions `review`/`block` : score, règles déclenchées, `model_version`, vélocités", "Illimitée"],
    ["`ml_features`", "`mongo_writer.py` (upsert)", "**Un document par client** : dernières features et dernière décision", "Écrasé à chaque transaction"],
    ["`ml_monitoring`", "`ml/monitor.py`", "Contrôles Evidently : dérive, précision servie, déclenchement de réentraînement", "Illimitée"],
    ["`logs`", "`mongo_writer.py`", "Traces techniques (service, niveau, durée)", "Champ `ttl_expires_at` à 90 jours"],
    ["`user_interactions`", "Aucun producteur", "Schéma et index prêts (clickstream)", "TTL 30 jours"],
    ["`customer_feedback`", "Aucun producteur", "Schéma prêt (contestations)", "—"],
  ], [20, 24, 40, 16]),
  h2("4.1 Exemple : fraud_alerts"),
  ...code(`{
  "txn_id": "747fefa2-7d1a-4579-8ccd-eb19d2abd76b",   // référence faible → PostgreSQL
  "customer_id": "…", "merchant_id": "…",
  "amount": 150000, "currency": "EUR",
  "ip_country": "RU", "device_type": "web",
  "fraud_score": 0.9912,
  "decision": "block",
  "rules_triggered": ["R1_high_amount", "R3_high_risk_geo"],  // embarqué, vide pour xgboost-v1
  "model_version": "rule-based-v1",
  "velocity_1h": 3, "velocity_24h": 11,
  "created_at": ISODate("2026-09-15T17:12:04Z")
}`),
  h2("4.2 Embedding ou referencing"),
  ...table(["Donnée", "Choix", "Raison"], [
    ["`rules_triggered` dans `fraud_alerts`", "Embedding (tableau)", "Toujours lu avec l’alerte ; ajouter une règle ne demande aucune migration"],
    ["`payload` dans `transaction_logs`", "Embedding", "Enquête sur une transaction en une seule lecture, sans jointure"],
    ["`drift`, `performance` dans `ml_monitoring`", "Embedding", "Un contrôle forme un tout"],
    ["`txn_id`, `customer_id`, `merchant_id`", "Referencing (UUID)", "Donnée maître dans PostgreSQL, cohérence assurée par le pipeline CDC"],
  ], [34, 20, 46]),
  ...image(img("stripe_mongodb_structure.png"), "Figure 3 — Structure MongoDB (presentation/stripe_mongodb_structure.drawio)"),

  // ─────────────────────────────────────────────────────────────────────────
  h1("5. Pipeline de données"),
  p("Deux canaux coexistent : un canal **temps réel** pour la fraude, et un canal **batch** pour l’analytique. Le CDC découple les deux de la base transactionnelle : la base n’appelle jamais Kafka, Debezium lit son journal de réplication."),
  h2("5.1 Composants"),
  ...table(["Composant", "Rôle", "Implémentation"], [
    ["CDC", "Capter chaque changement de `transactions`, `refunds`, `fraud_indicators`", "Debezium 2.6, plugin `pgoutput`, publication `stripe_publication`"],
    ["Bus", "Transport des événements", "Kafka en mode KRaft (1 broker en local, double listener 9092/29092)"],
    ["Scoring", "Features, score, décision, write-back", "`producers/flink_like_job.py` ; équivalent PyFlink dans `flink/fraud_scoring_job.py`"],
    ["Feature store online", "Vélocités 1 h / 24 h", "Redis 7 (ZSET glissants, HASH de features)"],
    ["Persistance NoSQL", "Kafka → MongoDB", "`producers/mongo_writer.py`, DLQ en cas d’échec"],
    ["Batch", "Export quotidien et rafraîchissement des vues", "Airflow 2.9, DAG `stripe_daily_etl` (02:00 UTC)"],
    ["OLAP", "Entrepôt analytique", "Snowflake (dry-run)"],
  ], [18, 38, 44]),
  h2("5.2 Topics Kafka"),
  ...table(["Topic", "Producteur", "Partitions / rétention"], [
    ["`stripe.public.transactions`", "Debezium (CDC)", "Créé par Debezium"],
    ["`stripe.public.refunds`, `stripe.public.fraud_indicators`", "Debezium (CDC)", "Créés par Debezium"],
    ["`stripe.payments.events`", "Scorer (toutes les transactions scorées)", "12 partitions, 30 jours"],
    ["`stripe.fraud.alerts`", "Scorer (`review`/`block`)", "3 partitions, 30 jours"],
    ["`stripe.etl.dead-letter`", "Scorer et mongo-writer (messages en erreur)", "3 partitions, rétention infinie"],
  ], [40, 36, 24]),
  h2("5.3 Flux temps réel"),
  ...numbered([
    "`transaction_producer.py` insère une transaction dans PostgreSQL.",
    "Debezium publie l’événement dans `stripe.public.transactions`.",
    "Le scorer lit la vélocité du client dans Redis (reconstruite depuis PostgreSQL si Redis a été vidé), calcule le score avec XGBoost, ou avec les règles si aucun modèle n’est disponible, puis décide : `block` au-delà de 0,85, `review` au-delà de 0,60, `allow` sinon.",
    "Il publie la transaction scorée dans `stripe.payments.events`, et dans `stripe.fraud.alerts` si elle est suspecte.",
    "Il écrit le score et l’éventuel indicateur dans PostgreSQL, dans une même transaction.",
    "`mongo_writer.py` alimente `transaction_logs`, `logs`, `ml_features` et `fraud_alerts`.",
    "Le dashboard Streamlit, protégé par login, lit PostgreSQL (rôle `analytics_reader`), MongoDB et Redis.",
  ]),
  h2("5.4 Flux batch"),
  bullets([
    "DAG Airflow `stripe_daily_etl`, planifié chaque jour à 02:00 UTC : tâche `snowflake_export` (`etl/load_snowflake.py`), puis `refresh_views` (`etl/refresh_views.py`).",
    "Dimensions chargées en upsert (`MERGE`), faits en insertion idempotente sur `txn_id`.",
    "Création du schéma Snowflake : amorçage manuel ponctuel (`make snowflake-setup`), volontairement hors du DAG quotidien.",
  ]),
  h2("5.5 Résilience du pipeline"),
  bullets([
    "**Dead-letter queue** : un message illisible ou une écriture en échec part dans `stripe.etl.dead-letter` avec son topic, sa partition et son offset ; le pipeline continue.",
    "**Reconnexion limitée** du scorer à PostgreSQL, arrêt propre sur signal.",
    "**Repli automatique** du scoring sur les règles si le modèle est absent.",
    "**Reconstruction de la vélocité** depuis l’historique PostgreSQL après une perte de Redis (incident documenté dans `docs/MLOPS.md` §5.2).",
  ]),

  // ─────────────────────────────────────────────────────────────────────────
  h1("6. Sécurité et conformité"),
  p("Les données de paiement imposent la sécurité comme contrainte de conception. Le tableau sépare ce qui est **en place dans le PoC** de ce qui est **codé pour la cible** (Terraform)."),
  h2("6.1 PCI-DSS v4.0"),
  ...table(["Exigence", "PoC local", "Cible (terraform/)"], [
    ["1.3 Isoler l’environnement de données de carte", "Réseau Docker interne", "Sous-réseaux data sans route Internet ; bases accessibles uniquement depuis le security group applicatif"],
    ["3.3 / 3.5 Protéger les données stockées", "Aucun PAN ; `fingerprint` pseudonymisé ; `pgcrypto` disponible", "CMK KMS à rotation annuelle sur RDS, MSK, Redis, S3, logs, ECR, secrets"],
    ["4.2 Chiffrer les transmissions", "TLS désactivé en démo (écart assumé et documenté)", "`rds.force_ssl=1`, MSK TLS obligatoire, Redis TLS, S3 refuse le non-TLS"],
    ["7 Restreindre l’accès", "Rôles `replication_user` (CDC), `analytics_reader` (sans `fingerprint`), `stripe_app` MongoDB en `readWrite` sur `stripe_nosql`", "Rôle de tâche ECS limité aux topics `stripe.*`, au data lake et à la clé"],
    ["8 Identifier et authentifier", "Secrets générés par `scripts/init_env.sh` dans `.env` (hors Git) ; dashboard : login, hash SHA-256, blocage après échecs", "Secrets Manager ; mot de passe RDS géré par RDS ; Kafka en IAM"],
    ["10 Journaliser", "Logs de services, collection `logs`", "VPC Flow Logs 365 jours, logs RDS/MSK/ECS dans CloudWatch chiffré"],
  ], [24, 38, 38]),
  h2("6.2 RGPD"),
  ...table(["Article", "Mécanisme", "Implémentation"], [
    ["Art. 17 — effacement", "Anonymisation sans suppression", "`anonymize_customer(uuid)` dans `init/postgres/02_rgpd.sql` : e-mail et nom remplacés, empreinte effacée, transactions détachées mais conservées (obligation comptable). Démontrée sous ROLLBACK dans `queries/postgres_oltp.sql`"],
    ["Art. 5(1)(e) — limitation de conservation", "Index TTL", "`transaction_logs` 90 jours, `user_interactions` 30 jours"],
    ["Art. 4(5) — pseudonymisation", "Empreinte", "`payment_methods.fingerprint`, jamais le PAN"],
    ["Art. 25 — protection dès la conception", "Moindre privilège", "`analytics_reader` sans accès à `fingerprint`"],
    ["Art. 44 — transferts", "Hébergement UE", "Cible en `eu-west-1`, Atlas en `EU_WEST_1`"],
  ], [24, 22, 54]),

  // ─────────────────────────────────────────────────────────────────────────
  h1("7. Intégration du Machine Learning"),
  p("La détection de fraude est le cas d’usage qui justifie l’architecture : la décision est prise pendant que le paiement attend. Détails : `docs/ML_INTEGRATION_STRATEGY.md` et `docs/MLOPS.md`."),
  h2("7.1 Deux moteurs, un contrat"),
  ...table(["Moteur", "Activation", "Logique", "Trace"], [
    ["Règles", "Défaut, et repli automatique", "5 règles additives : montant élevé (+0,35), card testing (+0,15), pays à risque (+0,40), vélocité 1 h > 10 (+0,25), vélocité 24 h > 50 (+0,15)", "`model_version = rule-based-v1`, règles listées"],
    ["XGBoost", "`SCORING_ENGINE=ml`, si un modèle existe", "200 arbres, profondeur 4, `scale_pos_weight` calculé à chaque entraînement", "`model_version = xgboost-v1`"],
  ], [14, 22, 44, 20]),
  h2("7.2 Features"),
  p("Le même vecteur est calculé à l’entraînement et à l’inférence par `ml/features.py`, ce qui évite tout décalage entre les deux :"),
  ...table(["Feature", "Source"], [
    ["`amount_log`", "Montant de la transaction (log)"],
    ["`hour_of_day`, `day_of_week`", "`created_at`"],
    ["`is_high_risk_country`", "`ip_country` comparé à la liste de pays à risque"],
    ["`is_pos_device`", "`device_type`"],
    ["`velocity_1h`, `velocity_24h`", "Redis en temps réel ; recalculées depuis PostgreSQL pour l’entraînement"],
  ], [35, 65]),
  h2("7.3 Cycle de vie"),
  bullets([
    "**Entraînement** : `ml/train_fraud_model.py`, découpage **temporel** 80/20 (pas aléatoire, pour ne pas faire fuiter les vélocités du futur), tracé dans MLflow (paramètres, métriques, modèle dans le registre `fraud-detector`).",
    "**Service** : `ml/scoring.py` recharge le modèle dès que le fichier change sur disque.",
    "**Surveillance** : `ml/monitor.py` (service `ml-monitor`) calcule la dérive avec Evidently et la **précision servie** sur les décisions réelles, puis relance un entraînement si la dérive dépasse 0,3 ou si la qualité baisse.",
    "**Audit** : `notebooks/audit_data_ml.ipynb`, exécuté en CI, échoue sur doublons CDC, features désalignées ou précision servie sous le seuil.",
  ]),
  h2("7.4 Résultats mesurés le 15/09/2026"),
  ...table(["Mesure", "Précision", "Rappel", "Contexte"], [
    ["Offline, entraînement de 15:53", "0,95", "0,97", "20 407 transactions de test, AUC 0,995"],
    ["Offline, réentraînement auto de 17:10", "0,77", "0,97", "21 744 transactions de test, fenêtre contenant un redémarrage de la stack"],
    ["**Servie**, 17:00-17:12", "**0,93**", "**0,96**", "1 225 vrais positifs, 92 faux positifs, 45 faux négatifs"],
  ], [34, 14, 14, 38]),
  note("Deux limites à connaître. (1) Les labels viennent du générateur synthétique ; en production, il faudrait des rétrofacturations (chargebacks) confirmées, qui arrivent 30 à 90 jours plus tard. (2) Le trafic scoré contient environ 25 % de fraude (rafales), contre 0,1 à 0,5 % dans la réalité : à rappel égal, la précision réelle serait plus basse. C’est pourquoi la précision servie est mesurée en continu, et pas seulement la métrique offline.", "warn"),

  // ─────────────────────────────────────────────────────────────────────────
  h1("8. Requêtes SQL et NoSQL"),
  p("Les requêtes sont dans `queries/` et exécutées sur la stack par `make queries-check` (et en CI) : une requête désalignée du schéma fait échouer la vérification."),
  ...table(["Fichier", "Contenu", "Exécution"], [
    ["`postgres_oltp.sql`", "Top marchands, décisions par moyen de paiement, précision/rappel servis, RFM, vélocité, remboursements en attente, vues matérialisées, EXPLAIN, RGPD sous ROLLBACK", "Oui"],
    ["`mongodb_queries.js`", "Alertes par décision et modèle, règles déclenchées, pays, volume horaire, historique d’une transaction, feature store, monitoring ML, latence, erreurs, index TTL", "Oui"],
    ["`snowflake_olap.sql`", "Région/trimestre, wallets vs cartes, pays à risque, GMV mois sur mois (LAG), Dynamic Table", "Non (dry-run)"],
  ], [22, 62, 16]),
  h2("8.1 SQL — précision et rappel servis (24 h)"),
  ...code(`WITH labelled AS (
    SELECT (t.metadata->>'is_fraud_pattern')::boolean  AS is_fraud,
           COALESCE(fi.decision = 'block', false)       AS predicted_fraud
    FROM transactions t
    LEFT JOIN fraud_indicators fi ON fi.txn_id = t.txn_id
    WHERE t.fraud_score IS NOT NULL
      AND t.updated_at >= NOW() - INTERVAL '24 hours'
      AND t.metadata ? 'is_fraud_pattern'
)
SELECT COUNT(*) FILTER (WHERE predicted_fraud AND is_fraud)     AS tp,
       COUNT(*) FILTER (WHERE predicted_fraud AND NOT is_fraud) AS fp,
       COUNT(*) FILTER (WHERE NOT predicted_fraud AND is_fraud) AS fn
FROM labelled;`),
  h2("8.2 MongoDB — alertes des dernières 24 h par décision et modèle"),
  ...code(`db.fraud_alerts.aggregate([
  { $match: { created_at: { $gte: since24h } } },
  { $group: { _id: { decision: "$decision", model_version: "$model_version" },
              alerts: { $sum: 1 }, avg_score: { $avg: "$fraud_score" } } },
  { $sort: { alerts: -1 } }
])`),
  h2("8.3 Snowflake — revenu et fraude par région et trimestre"),
  ...code(`SELECT g.region, d.year, d.quarter,
       SUM(f.amount_eur)    AS revenue_eur,
       COUNT_IF(f.is_fraud) AS fraud_count
FROM fact_transactions f
JOIN dim_date d      ON d.date_key = f.date_key
JOIN dim_geography g ON g.geo_key  = f.geo_key
WHERE f.date_key BETWEEN 20260101 AND 20261231
GROUP BY g.region, d.year, d.quarter;`),

  // ─────────────────────────────────────────────────────────────────────────
  h1("9. Qualité, tests et CI/CD"),
  ...table(["Vérification", "Commande", "Ce qu’elle garantit"], [
    ["Tests de bout en bout", "`make test`", "16 contrôles : schéma, idempotence, publication CDC, vues, Redis, MongoDB, TTL, scoring d’une fraude et trace `fraud_indicators`"],
    ["Requêtes", "`make queries-check`", "Les requêtes SQL et MongoDB s’exécutent sur le schéma réel"],
    ["Audit données/ML", "`make notebook-check`", "Pas de doublons CDC, features alignées, précision servie au-dessus du seuil"],
    ["Infrastructure as Code", "`make tf-validate`", "`terraform fmt` et `validate` sur bootstrap, dev et prod"],
    ["GitHub Actions", "`.github/workflows/ci.yml`", "Jobs `lint`, `e2e` (stack complète sur le runner, sans mocks) et `terraform`, à chaque push"],
  ], [22, 22, 56]),

  // ─────────────────────────────────────────────────────────────────────────
  h1("10. Du PoC à la cible cloud"),
  note("**Statut : écrit et validé, non appliqué.** Le dossier `terraform/` passe `terraform validate` en CI mais n’a jamais été déployé : aucun compte AWS ni Atlas n’est rattaché au projet.", "warn"),
  ...table(["PoC (Docker Compose)", "Cible AWS", "Module Terraform"], [
    ["postgres", "RDS PostgreSQL 16 Multi-AZ + réplica, réplication logique, TLS forcé", "`modules/rds`"],
    ["kafka + debezium", "MSK 3 brokers sur 3 AZ, RF 3, IAM + TLS (Debezium sur MSK Connect : hors périmètre)", "`modules/msk`"],
    ["redis", "ElastiCache Redis 7 Multi-AZ chiffré", "`modules/elasticache`"],
    ["mongo", "MongoDB Atlas 7 via PrivateLink", "`modules/mongodb_atlas`"],
    ["scorer, mongo-writer, ml-monitor, dashboard", "ECS Fargate ARM64, image unique sur ECR", "`modules/compute`"],
    ["airflow", "Amazon MWAA", "`modules/airflow`"],
    ["volumes, archive", "S3 chiffré, cycle de vie", "`modules/storage`"],
    [".env", "Secrets Manager + KMS + IAM", "`modules/security`"],
    ["réseau Docker", "VPC 3 AZ, sous-réseaux public/app/data, Flow Logs", "`modules/network`"],
  ], [26, 52, 22]),
  ...image(img("stripe_aws_cible.png"), "Figure 4 — Architecture physique de la cible AWS (presentation/stripe_aws_cible.drawio)"),
  h2("10.1 Coûts"),
  p("Ordre de grandeur en prix publics à la demande (`docs/FINOPS.md`) : **environ 3 100 $ par mois en production**, dont 65 % pour les quatre bases managées, et environ 1 000 $ en développement. Remplacer MWAA par EventBridge pour l’unique DAG et prendre des engagements d’un an ramènent la production vers 2 400 $. Les budgets et alertes sont codés dans Terraform."),

  // ─────────────────────────────────────────────────────────────────────────
  h1("11. Conclusion"),
  p("L’intérêt du projet tient moins à la liste des technologies qu’à la logique de chaque choix : PostgreSQL pour l’ACID et la conformité, un schéma en étoile pour l’analytique, MongoDB pour les documents évolutifs, Redis pour les features en milliseconde, et Kafka alimenté par CDC pour relier le tout sans coupler les systèmes."),
  p("Le PoC le démontre de bout en bout, tests à l’appui : une transaction est capturée, scorée, tracée dans PostgreSQL et MongoDB, et la qualité du modèle est mesurée sur ses décisions réelles. Le passage en production ne change pas cette logique : il remplace des conteneurs par des services managés, décrits en Terraform."),
  p("Les limites sont connues et documentées : Snowflake non connecté, Terraform non appliqué, TLS désactivé en local et labels de fraude synthétiques."),
  h2("Références"),
  ...bullets([
    "PostgreSQL — Logical Replication (postgresql.org/docs)",
    "Debezium — PostgreSQL Connector (debezium.io)",
    "MongoDB — Data Modeling, TTL Indexes (mongodb.com/docs)",
    "Snowflake — Clustering Keys, Dynamic Tables (docs.snowflake.com)",
    "PCI Security Standards Council — PCI DSS v4.0",
    "CNIL — Guide RGPD de l’équipe de développement",
    "AWS Well-Architected Framework — piliers Sécurité, Fiabilité, Optimisation des coûts, Durabilité",
  ]),
];

const doc = makeDoc({ headerLeft: "Stripe — Architecture de données unifiée", footerText: "Patrice Duclos — octobre 2026", children });
Packer.toBuffer(doc).then((b) => { fs.writeFileSync(OUT, b); console.log("written", OUT); });
