-- =====================================================================
-- Stripe Polyglot — Initialisation PostgreSQL (OLTP)
-- Exécuté automatiquement au 1er démarrage du conteneur postgres
-- =====================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =====================================================================
-- Schéma OLTP — tables métier
-- =====================================================================

CREATE TABLE merchants (
    merchant_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name          VARCHAR(255) NOT NULL,
    email         VARCHAR(255) NOT NULL UNIQUE,
    country_code  CHAR(2) NOT NULL,
    tier          VARCHAR(20) NOT NULL DEFAULT 'standard',
    status        VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE customers (
    customer_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email         VARCHAR(255) NOT NULL,
    name          VARCHAR(255),
    country_code  CHAR(2),
    segment       VARCHAR(50),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_segment ON customers(segment);

CREATE TABLE payment_methods (
    pm_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id   UUID NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    type          VARCHAR(30) NOT NULL,
    brand         VARCHAR(20),
    last4         CHAR(4),
    fingerprint   VARCHAR(64),
    is_default    BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at    DATE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_pm_customer ON payment_methods(customer_id);

CREATE TABLE transactions (
    txn_id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    merchant_id      UUID NOT NULL REFERENCES merchants(merchant_id),
    customer_id      UUID REFERENCES customers(customer_id),
    pm_id            UUID REFERENCES payment_methods(pm_id),
    amount           BIGINT NOT NULL,
    currency         CHAR(3) NOT NULL,
    status           VARCHAR(20) NOT NULL,
    fraud_score      NUMERIC(5,4),
    idempotency_key  VARCHAR(128) UNIQUE,
    device_type      VARCHAR(50),
    ip_country       CHAR(2),
    metadata         JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_txn_merchant   ON transactions(merchant_id, created_at DESC);
CREATE INDEX idx_txn_customer   ON transactions(customer_id, created_at DESC);
CREATE INDEX idx_txn_status     ON transactions(status);
CREATE INDEX idx_txn_created_at ON transactions(created_at DESC);
CREATE INDEX idx_txn_fraud      ON transactions(fraud_score) WHERE fraud_score IS NOT NULL;

CREATE TABLE refunds (
    refund_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    txn_id      UUID NOT NULL REFERENCES transactions(txn_id),
    amount      BIGINT NOT NULL,
    reason      VARCHAR(100),
    status      VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_refunds_txn ON refunds(txn_id);

CREATE TABLE fraud_indicators (
    fraud_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    txn_id          UUID NOT NULL REFERENCES transactions(txn_id),
    anomaly_score   NUMERIC(5,4) NOT NULL,
    rules_triggered TEXT[],
    model_version   VARCHAR(20),
    decision        VARCHAR(10) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_fraud_txn ON fraud_indicators(txn_id);
CREATE INDEX idx_fraud_decision ON fraud_indicators(decision, created_at DESC);

-- =====================================================================
-- Trigger updated_at
-- =====================================================================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_merchants_updated_at
  BEFORE UPDATE ON merchants
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_transactions_updated_at
  BEFORE UPDATE ON transactions
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- =====================================================================
-- Publication pour Debezium (CDC)
-- =====================================================================

-- Publication : Debezium ne lit QUE les tables qu'on liste ici
CREATE PUBLICATION stripe_publication FOR TABLE
    public.transactions,
    public.refunds,
    public.fraud_indicators;

-- =====================================================================
-- Vues matérialisées (analytics temps réel)
-- =====================================================================

-- Revenu quotidien par devise : une ligne par (jour, currency). Ne compte que
-- les transactions 'succeeded' — un échec ne génère pas de revenu réel.
-- WITH NO DATA : la vue est créée vide, à peupler explicitement avec
-- REFRESH MATERIALIZED VIEW (sinon l'init SQL scanne une table encore vide au 1er boot).
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT
    DATE_TRUNC('day', created_at) AS day,
    currency,
    COUNT(*) AS txn_count,
    SUM(amount) AS gross_amount_cents,
    -- Moyenne sur TOUTES les lignes du groupe (succeeded uniquement), y compris
    -- celles pas encore scorées (fraud_score NULL est ignoré par AVG() nativement).
    AVG(fraud_score) AS avg_fraud_score
FROM transactions
WHERE status = 'succeeded'
GROUP BY DATE_TRUNC('day', created_at), currency
WITH NO DATA;

-- Index UNIQUE requis pour permettre un REFRESH CONCURRENTLY (rafraîchissement
-- sans verrouiller les lecteurs pendant le recalcul).
CREATE UNIQUE INDEX idx_mv_daily_revenue ON mv_daily_revenue(day, currency);

-- Statistiques agrégées par marchand : txn_count + GMV + score fraude moyen.
CREATE MATERIALIZED VIEW mv_merchant_stats AS
SELECT
    m.merchant_id,
    m.name,
    m.tier,
    -- Compte toutes les transactions du marchand (succeeded ou non).
    COUNT(t.txn_id) AS txn_count,
    -- GMV (Gross Merchandise Value) : uniquement les paiements réussis.
    COALESCE(SUM(t.amount) FILTER (WHERE t.status = 'succeeded'), 0) AS gmv_cents,
    COALESCE(AVG(t.fraud_score), 0) AS avg_fraud_score
FROM merchants m
-- LEFT JOIN (pas INNER) : un marchand sans transaction doit quand même
-- apparaître dans la vue, avec txn_count=0 et gmv_cents=0 via COALESCE.
LEFT JOIN transactions t ON t.merchant_id = m.merchant_id
GROUP BY m.merchant_id, m.name, m.tier
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_merchant_stats ON mv_merchant_stats(merchant_id);

-- =====================================================================
-- Utilisateur de réplication (Debezium)
-- =====================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replication_user') THEN
        CREATE ROLE replication_user WITH REPLICATION LOGIN;
    END IF;
END $$;

-- Le mot de passe de replication_user sera défini par une migration séparée
-- (postgres_init_roles.sh) pour ne pas hardcoder dans le SQL init

GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO replication_user;

-- =====================================================================
-- Rôle analytics_reader (lecture seule, sans accès aux données sensibles)
-- cf. docs/SECURITY_COMPLIANCE_PLAN.md §3.2
-- =====================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analytics_reader') THEN
        CREATE ROLE analytics_reader WITH LOGIN;
    END IF;
END $$;

-- Mot de passe défini par postgres_init_roles.sh, même raison que replication_user.

-- Accès en lecture sur les tables métier sans donnée de paiement sensible.
GRANT SELECT ON merchants, customers, transactions, refunds, fraud_indicators TO analytics_reader;

-- payment_methods : accès colonne par colonne, fingerprint explicitement exclu
-- (c'est la seule colonne réellement sensible de la table, même pseudonymisée —
-- cf. §2 du plan de sécurité). GRANT liste les colonnes autorisées plutôt que
-- REVOKE une colonne précise, pour que l'ajout d'une future colonne sensible
-- à payment_methods ne soit pas accessible par défaut à ce rôle.
GRANT SELECT (pm_id, customer_id, type, brand, last4, is_default, expires_at, created_at)
    ON payment_methods TO analytics_reader;
