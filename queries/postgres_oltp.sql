-- =============================================================================
-- Stripe Polyglot — Requêtes SQL OLTP (PostgreSQL 16)
-- Livrable 8 de l'énoncé — AIA RNCP41993, Bloc 2
--
-- Toutes ces requêtes tournent sur la stack réelle (schéma
-- init/postgres/01_ddl.sql, données de seed/seed_data.py et
-- producers/transaction_producer.py, scores écrits par
-- producers/flink_like_job.py). Vérifiées par :
--     make queries-check
-- qui exécute ce fichier avec ON_ERROR_STOP=1 (toute erreur = échec).
--
-- Conventions : montants en centimes (BIGINT) → division par 100.0 à
-- l'affichage ; aucune conversion de devise (le PoC ne stocke pas de taux),
-- d'où le regroupement systématique par currency.
-- =============================================================================

\pset pager off
\timing on

-- -----------------------------------------------------------------------------
-- 1. Revenu mensuel des 10 premiers marchands (année en cours)
-- Usage : KPI financier du dashboard interne.
-- Index utilisé : idx_txn_merchant (merchant_id, created_at DESC).
-- -----------------------------------------------------------------------------
\echo '== 1. Top 10 marchands par revenu mensuel =='
SELECT
    m.name                                   AS merchant,
    m.country_code,
    DATE_TRUNC('month', t.created_at)::date  AS month,
    t.currency,
    COUNT(*)                                 AS txn_count,
    ROUND(SUM(t.amount) / 100.0, 2)          AS revenue,
    ROUND(AVG(t.amount) / 100.0, 2)          AS avg_basket
FROM transactions t
JOIN merchants m ON m.merchant_id = t.merchant_id
WHERE t.status = 'succeeded'
  AND t.created_at >= DATE_TRUNC('year', NOW())
GROUP BY m.name, m.country_code, month, t.currency
ORDER BY revenue DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 2. Taux de review / block par type et marque de moyen de paiement (30 j)
-- Usage : équipe Risk, calibration des seuils 0.60 / 0.85.
-- Source des décisions : fraud_indicators, écrite par le scorer dans la même
-- transaction que transactions.fraud_score (write-back atomique).
-- -----------------------------------------------------------------------------
\echo '== 2. Décisions fraude par moyen de paiement (30 j) =='
SELECT
    pm.type,
    pm.brand,
    COUNT(*)                                                  AS txn_count,
    COUNT(*) FILTER (WHERE fi.decision = 'review')            AS review_count,
    COUNT(*) FILTER (WHERE fi.decision = 'block')             AS block_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE fi.decision = 'block')
          / NULLIF(COUNT(*), 0), 2)                           AS block_rate_pct
FROM transactions t
JOIN payment_methods pm       ON pm.pm_id = t.pm_id
LEFT JOIN fraud_indicators fi ON fi.txn_id = t.txn_id
WHERE t.created_at >= NOW() - INTERVAL '30 days'
GROUP BY pm.type, pm.brand
ORDER BY block_rate_pct DESC NULLS LAST;

-- -----------------------------------------------------------------------------
-- 3. Précision / rappel servis par version de modèle (24 h)
-- Usage : même mesure que ml/monitor.py::compute_served_performance, en SQL.
-- Vérité terrain : metadata->>'is_fraud_pattern' posé par le producteur
-- synthétique (en production : chargebacks confirmés, arrivant J+30 à J+90).
-- Prédiction positive = décision 'block' (score >= 0.85).
-- -----------------------------------------------------------------------------
\echo '== 3. Performance servie par model_version (24 h) =='
WITH labelled AS (
    SELECT
        COALESCE(fi.model_version, 'allow (pas d''indicateur)')   AS model_version,
        (t.metadata->>'is_fraud_pattern')::boolean                AS is_fraud,
        COALESCE(fi.decision = 'block', false)                    AS predicted_fraud
    FROM transactions t
    LEFT JOIN fraud_indicators fi ON fi.txn_id = t.txn_id
    WHERE t.fraud_score IS NOT NULL
      AND t.updated_at >= NOW() - INTERVAL '24 hours'
      AND t.metadata ? 'is_fraud_pattern'
)
SELECT
    COUNT(*)                                                   AS scored,
    COUNT(*) FILTER (WHERE predicted_fraud AND is_fraud)       AS tp,
    COUNT(*) FILTER (WHERE predicted_fraud AND NOT is_fraud)   AS fp,
    COUNT(*) FILTER (WHERE NOT predicted_fraud AND is_fraud)   AS fn,
    ROUND(COUNT(*) FILTER (WHERE predicted_fraud AND is_fraud)::numeric
          / NULLIF(COUNT(*) FILTER (WHERE predicted_fraud), 0), 3) AS precision,
    ROUND(COUNT(*) FILTER (WHERE predicted_fraud AND is_fraud)::numeric
          / NULLIF(COUNT(*) FILTER (WHERE is_fraud), 0), 3)        AS recall
FROM labelled;

-- -----------------------------------------------------------------------------
-- 4. Segmentation RFM des clients (Recency, Frequency, Monetary)
-- Usage : marketing, feature candidate pour le modèle de fraude.
-- -----------------------------------------------------------------------------
\echo '== 4. Segmentation RFM (20 premiers clients) =='
WITH rfm AS (
    SELECT
        customer_id,
        MAX(created_at)        AS last_txn_at,
        COUNT(*)               AS frequency,
        SUM(amount) / 100.0    AS monetary
    FROM transactions
    WHERE status = 'succeeded'
      AND customer_id IS NOT NULL        -- clients anonymisés RGPD exclus
    GROUP BY customer_id
)
SELECT
    customer_id,
    EXTRACT(DAY FROM NOW() - last_txn_at)::int  AS recency_days,
    frequency,
    ROUND(monetary, 2)                          AS monetary,
    CASE
        WHEN frequency > 50 AND monetary > 5000 THEN 'VIP'
        WHEN frequency > 10 AND monetary > 1000 THEN 'Regular'
        WHEN frequency > 2                      THEN 'Occasional'
        ELSE 'New'
    END                                         AS segment
FROM rfm
ORDER BY monetary DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 5. Vélocité suspecte sur la dernière heure
-- Usage : contrôle a posteriori de la règle R4 du scorer (> 10 txns/h,
-- calculée en temps réel dans Redis) et détection multi-cartes / multi-pays.
-- Index utilisé : idx_txn_customer (customer_id, created_at DESC).
-- -----------------------------------------------------------------------------
\echo '== 5. Clients à vélocité suspecte (1 h) =='
SELECT
    t.customer_id,
    COUNT(*)                         AS txn_count_1h,
    ROUND(SUM(t.amount) / 100.0, 2)  AS total_1h,
    COUNT(DISTINCT t.ip_country)     AS distinct_countries,
    COUNT(DISTINCT pm.fingerprint)   AS distinct_cards
FROM transactions t
LEFT JOIN payment_methods pm ON pm.pm_id = t.pm_id
WHERE t.created_at >= NOW() - INTERVAL '1 hour'
  AND t.customer_id IS NOT NULL
GROUP BY t.customer_id
HAVING COUNT(*) > 10
    OR COUNT(DISTINCT t.ip_country) > 2
    OR COUNT(DISTINCT pm.fingerprint) > 3
ORDER BY txn_count_1h DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 6. Remboursements en attente depuis plus de 48 h (SLA support)
-- Note : le producteur synthétique ne génère pas encore de remboursements ;
-- la requête est valide et retourne 0 ligne tant que refunds est vide.
-- -----------------------------------------------------------------------------
\echo '== 6. Remboursements en attente > 48 h =='
SELECT
    r.refund_id,
    r.txn_id,
    ROUND(r.amount / 100.0, 2)                      AS refund_amount,
    t.currency,
    r.reason,
    EXTRACT(EPOCH FROM NOW() - r.created_at) / 3600 AS hours_pending,
    m.name                                          AS merchant,
    m.tier
FROM refunds r
JOIN transactions t ON t.txn_id = r.txn_id
JOIN merchants m    ON m.merchant_id = t.merchant_id
WHERE r.status = 'pending'
  AND r.created_at < NOW() - INTERVAL '48 hours'
ORDER BY r.created_at;

-- -----------------------------------------------------------------------------
-- 7. Vues matérialisées PostgreSQL (pré-agrégats côté OLTP)
-- mv_daily_revenue et mv_merchant_stats sont des vues PostgreSQL, rafraîchies
-- par etl/refresh_views.py (tâche refresh_views du DAG Airflow). Elles servent
-- le dashboard temps réel ; l'analytique historique relève de Snowflake
-- (queries/snowflake_olap.sql).
-- -----------------------------------------------------------------------------
\echo '== 7a. Revenu journalier (mv_daily_revenue, 7 derniers jours) =='
SELECT
    day::date,
    currency,
    txn_count,
    ROUND(gross_amount_cents / 100.0, 2) AS gross_amount,
    ROUND(avg_fraud_score, 4)            AS avg_fraud_score
FROM mv_daily_revenue
WHERE day >= NOW() - INTERVAL '7 days'
ORDER BY day DESC, gross_amount DESC;

\echo '== 7b. Marchands à risque (mv_merchant_stats) =='
SELECT
    name,
    tier,
    txn_count,
    ROUND(gmv_cents / 100.0, 2)  AS gmv,
    ROUND(avg_fraud_score, 4)    AS avg_fraud_score
FROM mv_merchant_stats
WHERE txn_count >= 20
ORDER BY avg_fraud_score DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 8. Plan d'exécution de la déduplication par clé d'idempotence
-- Montre que la recherche passe par l'index UNIQUE (Index Scan) et non par
-- un parcours complet de la table.
-- -----------------------------------------------------------------------------
\echo '== 8. EXPLAIN — lookup par idempotency_key =='
EXPLAIN (COSTS OFF)
SELECT txn_id FROM transactions WHERE idempotency_key = 'demo-key';

-- -----------------------------------------------------------------------------
-- 9. Droit à l'effacement RGPD (art. 17) — démonstration sans effet
-- anonymize_customer() est définie dans init/postgres/02_rgpd.sql.
-- Exécutée dans une transaction annulée : on montre l'effet puis ROLLBACK,
-- aucune donnée n'est modifiée par make queries-check.
-- -----------------------------------------------------------------------------
\echo '== 9. RGPD — anonymisation d''un client (ROLLBACK) =='
BEGIN;

SELECT customer_id AS target_customer
FROM transactions
WHERE customer_id IS NOT NULL
ORDER BY created_at DESC
LIMIT 1
\gset

SELECT anonymize_customer(:'target_customer'::uuid) AS transactions_detached;

SELECT customer_id, email, name
FROM customers
WHERE customer_id = :'target_customer'::uuid;

SELECT COUNT(*) AS remaining_linked_transactions
FROM transactions
WHERE customer_id = :'target_customer'::uuid;

ROLLBACK;
