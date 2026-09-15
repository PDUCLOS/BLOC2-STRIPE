-- =====================================================================
-- Stripe Polyglot — Fonctions RGPD (PostgreSQL)
-- Exécuté au 1er démarrage du conteneur postgres, après 01_ddl.sql.
-- Référence : docs/SECURITY_COMPLIANCE_PLAN.md (art. 17 RGPD).
-- =====================================================================

-- anonymize_customer : droit à l'effacement sans casser l'intégrité.
--
-- Ce qu'on NE fait PAS : DELETE du client. Les transactions sont des
-- pièces comptables (conservation légale 10 ans, art. L123-22 du Code de
-- commerce) et fraud_indicators en dépend : on ne peut ni les supprimer
-- ni laisser une clé étrangère orpheline.
--
-- Ce qu'on fait, dans une seule transaction :
--   1. customers        : email / nom remplacés, pays effacé
--   2. payment_methods  : fingerprint et last4 remplacés (plus de lien avec la carte)
--   3. transactions     : customer_id détaché (NULL) — montants et dates conservés
--
-- Effet de bord maîtrisé : l'UPDATE de transactions génère un événement CDC.
-- Le scorer (producers/flink_like_job.py) ignore les transactions sans
-- customer_id et le write-back ne réécrit jamais un fraud_score déjà posé.
--
-- Côté MongoDB, les documents liés expirent via les index TTL
-- (transaction_logs 90 j, user_interactions 30 j).
--
-- Retourne le nombre de transactions détachées.
CREATE OR REPLACE FUNCTION anonymize_customer(p_customer_id UUID)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_detached INTEGER;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM customers WHERE customer_id = p_customer_id) THEN
        RAISE EXCEPTION 'anonymize_customer: client % introuvable', p_customer_id;
    END IF;

    UPDATE customers
    SET email        = 'anonymized_' || LEFT(md5(p_customer_id::text), 8) || '@deleted.invalid',
        name         = 'ANONYMIZED',
        country_code = NULL
    WHERE customer_id = p_customer_id;

    UPDATE payment_methods
    SET fingerprint = 'REDACTED',
        last4       = NULL
    WHERE customer_id = p_customer_id;

    UPDATE transactions
    SET customer_id = NULL
    WHERE customer_id = p_customer_id;
    GET DIAGNOSTICS v_detached = ROW_COUNT;

    RETURN v_detached;
END;
$$;

COMMENT ON FUNCTION anonymize_customer(UUID) IS
    'RGPD art. 17 : anonymise un client et détache ses transactions sans les supprimer';
