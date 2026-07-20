#!/usr/bin/env python3
"""
ETL batch PostgreSQL → Snowflake (OLAP).
Charge les transactions succeeded du jour vers fact_transactions.
Conçu pour tourner dans un DAG Airflow (make snowflake-export pour la démo).

Usage : make snowflake-export
"""
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    import snowflake.connector
except ImportError as e:
    print(f"❌ Dépendance manquante : {e}")
    sys.exit(1)

PG_CONFIG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", 5432)),
    dbname=os.environ.get("PG_DB", "stripe_oltp"),
    user=os.environ.get("PG_USER", "stripe_app"),
    password=os.environ.get("PG_PASSWORD", ""),
)
SF_ACCOUNT  = os.environ.get("SNOWFLAKE_ACCOUNT", "")
SF_USER     = os.environ.get("SNOWFLAKE_USER", "")
SF_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "")
SF_WH       = os.environ.get("SNOWFLAKE_WAREHOUSE", "STRIPE_WH")
SF_DB       = os.environ.get("SNOWFLAKE_DB", "STRIPE_DWH")
SF_SCHEMA   = os.environ.get("SNOWFLAKE_SCHEMA", "PROD")

BATCH_SIZE = 5000


def extract_from_pg(target_date: date):
    """Extrait les transactions succeeded du jour spécifié depuis PostgreSQL.
    
    Args:
        target_date (date): La date pour laquelle extraire les transactions.
        
    Returns:
        list[dict]: Liste de dictionnaires contenant les données des transactions extraites.
    """
    # Extraction bornée sur la journée pour des batches prévisibles et rejouables.
    conn = psycopg2.connect(**PG_CONFIG)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                t.txn_id,
                t.merchant_id,
                t.customer_id,
                t.pm_id,
                t.amount,
                t.currency,
                t.status,
                t.fraud_score,
                t.device_type,
                t.ip_country,
                t.created_at,
                pm.type AS pm_type,
                pm.brand AS pm_brand
            FROM transactions t
            LEFT JOIN payment_methods pm ON pm.pm_id = t.pm_id
            WHERE DATE(t.created_at) = %s
              AND t.status = 'succeeded'
            ORDER BY t.created_at
        """, (target_date,))
        rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_dimensions(sf_cur, rows):
    """Upsert (Merge/Insert) des dimensions depuis les données de transaction.
    
    Cette fonction met à jour ou insère les enregistrements dans les tables de dimensions
    (merchants, customers, payment_methods) pour garantir que les clés étrangères de la
    table de faits seront valides.
    
    Args:
        sf_cur: Curseur Snowflake.
        rows (list[dict]): Liste des transactions contenant les informations des dimensions.
    """
    # dim_merchant — MERGE depuis PostgreSQL direct
    sf_cur.execute("""
        MERGE INTO dim_merchant tgt
        USING (
            SELECT DISTINCT
                merchant_id,
                country_code,
                tier,
                status
            FROM VALUES %s AS v(merchant_id, country_code, tier, status)
        ) src ON tgt.merchant_id = src.merchant_id
        WHEN NOT MATCHED THEN
            INSERT (merchant_id, country_code, tier, status)
            VALUES (src.merchant_id, src.country_code, src.tier, src.status)
    """)  # Simplifié pour la démo — en prod on ferait un JOIN sur PG

    # dim_customer
    customer_vals = list({
        r["customer_id"]: (r["customer_id"],)
        for r in rows if r.get("customer_id")
    }.values())
    if customer_vals:
        sf_cur.executemany(
            "INSERT INTO dim_customer (customer_id) SELECT %s WHERE NOT EXISTS (SELECT 1 FROM dim_customer WHERE customer_id = %s)",
            [(c[0], c[0]) for c in customer_vals]
        )

    # dim_payment_method
    pm_vals = list({
        r["pm_id"]: (r["pm_id"], r.get("pm_type", "card"), r.get("pm_brand"))
        for r in rows if r.get("pm_id")
    }.values())
    if pm_vals:
        sf_cur.executemany(
            "INSERT INTO dim_payment_method (pm_id, type, brand) SELECT %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM dim_payment_method WHERE pm_id = %s)",
            [(p[0], p[1], p[2], p[0]) for p in pm_vals]
        )


def load_to_snowflake(rows, target_date: date):
    """Charge les lignes dans la table fact_transactions via MERGE (idempotent).
    
    La fonction procède par lots (batches) définis par BATCH_SIZE pour éviter de
    surcharger la mémoire ou d'atteindre les limites de taille de requête.
    
    Args:
        rows (list[dict]): Liste des transactions à insérer.
        target_date (date): La date de traitement (pour l'affichage/log).
    """
    if not SF_ACCOUNT:
        print("⚠️  SNOWFLAKE_ACCOUNT non défini — export simulé (dry-run)")
        print(f"   {len(rows)} lignes SERAIENT chargées pour {target_date}")
        return

    sf = snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        warehouse=SF_WH, database=SF_DB, schema=SF_SCHEMA,
    )
    cur = sf.cursor()

    # Construction de la date_key
    def date_key(ts):
        if ts is None:
            return None
        d = ts.date() if hasattr(ts, "date") else ts
        return int(d.strftime("%Y%m%d"))

    loaded = 0
    # Traitement par lots pour contrôler mémoire et temps d'exécution côté driver.
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i: i + BATCH_SIZE]
        values = []
        for r in batch:
            # fee = 1.4% Stripe standard (démo)
            amount_eur = round(float(r["amount"]) / 100, 2)
            fee_amount = round(amount_eur * 0.014, 4)
            # processing_ms simulé : 20-45ms (démo)
            import random; random.seed(str(r["txn_id"]))
            processing_ms = round(20 + random.random() * 25, 1)

            values.append((
                str(r["txn_id"]),
                str(r["merchant_id"]) if r["merchant_id"] else None,
                str(r["customer_id"]) if r["customer_id"] else None,
                str(r["pm_id"]) if r["pm_id"] else None,
                date_key(r["created_at"]),
                r.get("ip_country"),
                amount_eur,
                fee_amount,
                r["currency"],
                r["status"],
                float(r["fraud_score"]) if r.get("fraud_score") is not None else None,
                bool(r.get("fraud_score", 0) and float(r["fraud_score"]) >= 0.85),
                r.get("device_type"),
                processing_ms,
                r["created_at"],
            ))

        # MERGE sur txn_id : idempotent en cas de relance du même batch.
        cur.executemany("""
            MERGE INTO fact_transactions tgt
            USING (SELECT
                %s AS txn_id, %s AS mid, %s AS cid, %s AS pmid,
                %s AS dkey, %s AS country, %s AS amount, %s AS fee,
                %s AS currency, %s AS status, %s AS fscore, %s AS is_fraud,
                %s AS device, %s AS proc_ms, %s AS created_at
            ) src ON tgt.txn_id = src.txn_id
            WHEN NOT MATCHED THEN INSERT (
                txn_id, merchant_key, customer_key, pm_key, date_key, geo_key,
                amount_eur, fee_amount, currency, status, fraud_score, is_fraud,
                device_type, processing_ms, created_at
            )
            VALUES (
                src.txn_id,
                (SELECT merchant_key FROM dim_merchant WHERE merchant_id = src.mid),
                (SELECT customer_key FROM dim_customer WHERE customer_id = src.cid),
                (SELECT pm_key FROM dim_payment_method WHERE pm_id = src.pmid),
                src.dkey,
                (SELECT geo_key FROM dim_geography WHERE country_code = src.country),
                src.amount, src.fee, src.currency, src.status, src.fscore, src.is_fraud,
                src.device, src.proc_ms, src.created_at
            )
        """, values)
        loaded += len(batch)
        print(f"  → {loaded}/{len(rows)} lignes chargées")

    sf.commit()
    cur.close()
    sf.close()
    print(f"✅ {loaded} transactions chargées dans Snowflake pour {target_date}")


def main():
    """Point d'entrée principal du processus ETL.
    
    Orchestre l'extraction des données depuis PostgreSQL, l'insertion éventuelle des dimensions,
    et le chargement des faits dans Snowflake. Conçu pour être exécuté quotidiennement.
    """
    target = date.today()
    print(f"🚀 ETL PostgreSQL → Snowflake — {target}")
    print(f"   Source : {PG_CONFIG['host']}/{PG_CONFIG['dbname']}")
    print(f"   Cible  : {SF_DB}.{SF_SCHEMA}.fact_transactions")
    print()

    print("📤 Extraction depuis PostgreSQL...")
    rows = extract_from_pg(target)
    print(f"  → {len(rows)} transactions succeeded pour {target}")

    if not rows:
        print("ℹ️  Aucune transaction à charger — le producer tourne-t-il ?")
        return

    print("📥 Chargement vers Snowflake...")
    load_to_snowflake(rows, target)


if __name__ == "__main__":
    main()
