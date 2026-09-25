-- =============================================================================
-- Stripe Polyglot — Requêtes OLAP (Snowflake, schéma en étoile)
-- Livrable 8 de l'énoncé — AIA RNCP41993, Bloc 2
--
-- Alignées sur le DDL réel d'etl/snowflake_setup.py :
--   fact_transactions (amount_eur, fee_amount, fraud_score, is_fraud,
--                      processing_ms, CLUSTER BY (date_key, merchant_key))
--   dim_date, dim_merchant, dim_customer, dim_payment_method, dim_geography
--
-- Exécutées sur le compte d'essai depuis le 25/09/2026 : make snowflake-setup
-- && make snowflake-export, puis make snowflake-check (etl/snowflake_queries_check.py)
-- qui rejoue ce fichier statement par statement et échoue à la première erreur.
-- Sans SNOWFLAKE_ACCOUNT (CI), la vérification est sautée.
-- Les pré-agrégats temps réel côté PostgreSQL (mv_daily_revenue,
-- mv_merchant_stats) sont dans queries/postgres_oltp.sql §7.
-- =============================================================================

-- Même base et schéma que etl/snowflake_setup.py (SNOWFLAKE_DB / SNOWFLAKE_SCHEMA)
USE SCHEMA STRIPE_DWH.PROD;

-- -----------------------------------------------------------------------------
-- 1. Revenu, commissions et fraude par région et trimestre
-- Usage : reporting exécutif. Le filtre sur date_key profite du clustering.
-- -----------------------------------------------------------------------------
SELECT
    g.region,
    d.year,
    d.quarter,
    COUNT(*)                                             AS txn_count,
    SUM(f.amount_eur)                                    AS revenue_eur,
    SUM(f.fee_amount)                                    AS fees_eur,
    COUNT_IF(f.is_fraud)                                 AS fraud_count,
    ROUND(100 * COUNT_IF(f.is_fraud) / NULLIF(COUNT(*), 0), 3) AS fraud_pct,
    ROUND(AVG(f.processing_ms), 1)                       AS avg_processing_ms
FROM fact_transactions f
JOIN dim_date d      ON d.date_key = f.date_key
JOIN dim_geography g ON g.geo_key  = f.geo_key
WHERE f.date_key BETWEEN 20260101 AND 20261231
GROUP BY g.region, d.year, d.quarter
ORDER BY d.year, d.quarter, revenue_eur DESC;

-- -----------------------------------------------------------------------------
-- 2. Portefeuilles numériques vs cartes classiques
-- -----------------------------------------------------------------------------
SELECT
    pm.type,
    pm.brand,
    pm.is_digital_wallet,
    COUNT(*)                         AS txn_count,
    SUM(f.amount_eur)                AS total_eur,
    ROUND(AVG(f.amount_eur), 2)      AS avg_basket_eur,
    COUNT_IF(f.is_fraud)             AS fraud_count,
    ROUND(AVG(f.fraud_score), 4)     AS avg_fraud_score
FROM fact_transactions f
JOIN dim_payment_method pm ON pm.pm_key = f.pm_key
JOIN dim_date d            ON d.date_key = f.date_key
WHERE d.year = 2026
GROUP BY pm.type, pm.brand, pm.is_digital_wallet
ORDER BY txn_count DESC;

-- -----------------------------------------------------------------------------
-- 3. Pays à haut risque : part de la fraude et du volume
-- dim_geography.is_high_risk reflète la liste HIGH_RISK_COUNTRIES du scorer.
-- -----------------------------------------------------------------------------
SELECT
    g.is_high_risk,
    g.country_code,
    COUNT(*)                                                    AS txn_count,
    COUNT_IF(f.is_fraud)                                        AS fraud_count,
    RATIO_TO_REPORT(COUNT_IF(f.is_fraud)) OVER ()               AS share_of_all_fraud,
    RATIO_TO_REPORT(COUNT(*)) OVER ()                           AS share_of_volume
FROM fact_transactions f
JOIN dim_geography g ON g.geo_key = f.geo_key
GROUP BY g.is_high_risk, g.country_code
ORDER BY fraud_count DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 4. Marchands : GMV mensuel et évolution mois sur mois (fenêtre LAG)
-- -----------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        m.merchant_id,
        m.name,
        m.tier,
        d.year,
        d.month,
        SUM(f.amount_eur) AS gmv_eur
    FROM fact_transactions f
    JOIN dim_merchant m ON m.merchant_key = f.merchant_key
    JOIN dim_date d     ON d.date_key     = f.date_key
    WHERE f.status = 'succeeded'
    GROUP BY m.merchant_id, m.name, m.tier, d.year, d.month
)
SELECT
    name,
    tier,
    year,
    month,
    gmv_eur,
    LAG(gmv_eur) OVER (PARTITION BY merchant_id ORDER BY year, month) AS prev_month_gmv_eur,
    ROUND(100 * (gmv_eur / NULLIF(LAG(gmv_eur) OVER (PARTITION BY merchant_id ORDER BY year, month), 0) - 1), 1)
                                                                       AS mom_growth_pct
FROM monthly
QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant_id ORDER BY year DESC, month DESC) = 1
ORDER BY gmv_eur DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 5. Pré-agrégat OLAP : Dynamic Table de revenu quotidien (cible)
-- Équivalent Snowflake de mv_daily_revenue (PostgreSQL) : Snowflake la
-- rafraîchit seul avec un retard maximal de 1 h, sans job de REFRESH.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE dt_daily_revenue
    TARGET_LAG = '1 hour'
    WAREHOUSE  = STRIPE_WH
AS
SELECT
    d.full_date,
    g.region,
    f.currency,
    COUNT(*)              AS txn_count,
    SUM(f.amount_eur)     AS revenue_eur,
    COUNT_IF(f.is_fraud)  AS fraud_count
FROM fact_transactions f
JOIN dim_date d      ON d.date_key = f.date_key
JOIN dim_geography g ON g.geo_key  = f.geo_key
WHERE f.status = 'succeeded'
GROUP BY d.full_date, g.region, f.currency;

SELECT full_date, region, currency, revenue_eur, fraud_count
FROM dt_daily_revenue
WHERE full_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY full_date DESC, revenue_eur DESC;
