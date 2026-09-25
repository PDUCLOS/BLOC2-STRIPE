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
    print(f"[ERROR] Dépendance manquante : {e}")
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
                t.txn_id, t.merchant_id, t.customer_id, t.pm_id,
                t.amount, t.currency, t.status, t.fraud_score,
                t.device_type, t.ip_country, t.created_at,
                -- Dénormalise le type/marque du moyen de paiement directement dans
                -- l'extraction : évite un JOIN côté Snowflake au moment du reporting.
                pm.type AS pm_type,
                pm.brand AS pm_brand,
                -- Attributs marchand nécessaires à upsert_dimensions() (dim_merchant) —
                -- extraits ici plutôt que re-requêtés côté Snowflake, même logique
                -- de dénormalisation que pour le moyen de paiement ci-dessus.
                m.name AS merchant_name,
                m.email AS merchant_email,
                m.country_code AS merchant_country_code,
                m.tier AS merchant_tier,
                m.status AS merchant_status
            FROM transactions t
            -- LEFT JOIN (pas INNER) : pm_id peut être NULL (moyen de paiement supprimé
            -- ou transaction sans pm associé) sans que la transaction disparaisse de l'export.
            LEFT JOIN payment_methods pm ON pm.pm_id = t.pm_id
            JOIN merchants m ON m.merchant_id = t.merchant_id
            -- Fenêtre = une journée calendaire complète (le job est pensé pour tourner
            -- une fois par jour, cf. Airflow 02:00 UTC dans PRESENTATION.md).
            WHERE DATE(t.created_at) = %s
              -- Seules les transactions abouties entrent dans le DWH financier —
              -- les échecs/pending n'ont pas leur place dans fact_transactions.
              AND t.status = 'succeeded'
            ORDER BY t.created_at
        """, (target_date,))
        rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


STAGING_TABLE = "stg_transactions"


def create_staging(sf_cur):
    """Crée la table temporaire de transit du chargement (durée de vie : la session).

    Le connecteur Snowflake ne sait réécrire en multi-lignes qu'un
    `INSERT ... VALUES (%s, ...)` : un `executemany` sur un `MERGE` ou un
    `INSERT ... SELECT ... WHERE NOT EXISTS` échoue (erreur 252001, constatée
    au premier export réel du 25/09/2026 — le dry-run ne pouvait pas le voir).
    On charge donc chaque lot dans cette table, puis dimensions et faits sont
    alimentés par des requêtes ensemblistes, en une instruction chacune.
    """
    sf_cur.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE {STAGING_TABLE} (
            txn_id VARCHAR(36), merchant_id VARCHAR(36), customer_id VARCHAR(36), pm_id VARCHAR(36),
            date_key NUMBER, country VARCHAR(2), amount_eur NUMBER(18,2), fee_amount NUMBER(18,4),
            currency CHAR(3), status VARCHAR(20), fraud_score NUMBER(5,4), is_fraud BOOLEAN,
            device_type VARCHAR(50), processing_ms NUMBER(8,2), created_at TIMESTAMP_TZ,
            merchant_name VARCHAR(255), merchant_email VARCHAR(255), merchant_country VARCHAR(2),
            merchant_tier VARCHAR(20), merchant_status VARCHAR(20), pm_type VARCHAR(30), pm_brand VARCHAR(20)
        )
    """)


def upsert_dimensions(sf_cur):
    """Insère les dimensions référencées par le lot présent dans la table de transit.

    Nécessaire AVANT le MERGE de fact_transactions : les sous-requêtes
    `(SELECT merchant_key FROM dim_merchant WHERE merchant_id = ...)` de
    load_to_snowflake() renvoient NULL si la ligne dimension n'existe pas
    encore — sans cet appel, merchant_key/customer_key/pm_key seraient NULL
    sur tous les faits chargés. Colonne Snowflake = "country" (cf.
    snowflake_setup.py), pas "country_code" comme côté Postgres.
    """
    sf_cur.execute(f"""
        INSERT INTO dim_merchant (merchant_id, name, email, country, tier, status)
        SELECT DISTINCT s.merchant_id, s.merchant_name, s.merchant_email, s.merchant_country,
               s.merchant_tier, s.merchant_status
        FROM {STAGING_TABLE} s
        WHERE s.merchant_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM dim_merchant d WHERE d.merchant_id = s.merchant_id)
    """)
    sf_cur.execute(f"""
        INSERT INTO dim_customer (customer_id)
        SELECT DISTINCT s.customer_id FROM {STAGING_TABLE} s
        WHERE s.customer_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM dim_customer d WHERE d.customer_id = s.customer_id)
    """)
    sf_cur.execute(f"""
        INSERT INTO dim_payment_method (pm_id, type, brand)
        SELECT DISTINCT s.pm_id, COALESCE(s.pm_type, 'card'), s.pm_brand FROM {STAGING_TABLE} s
        WHERE s.pm_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM dim_payment_method d WHERE d.pm_id = s.pm_id)
    """)


def load_to_snowflake(rows, target_date: date):
    """Charge les lignes dans la table fact_transactions via MERGE (idempotent).

    Par lots de BATCH_SIZE : chaque lot transite par une table temporaire,
    puis les dimensions manquantes sont insérées et les faits fusionnés sur
    txn_id, en une instruction ensembliste par table.

    Args:
        rows (list[dict]): Liste des transactions à insérer.
        target_date (date): La date de traitement (pour l'affichage/log).
    """
    if not SF_ACCOUNT:
        print("[WARN] SNOWFLAKE_ACCOUNT non défini — export simulé (dry-run)")
        print(f"   {len(rows)} lignes SERAIENT chargées pour {target_date}")
        return

    sf = snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        warehouse=SF_WH, database=SF_DB, schema=SF_SCHEMA,
    )
    cur = sf.cursor()
    create_staging(cur)

    # Construction de la date_key
    def date_key(ts):
        if ts is None:
            return None
        d = ts.date() if hasattr(ts, "date") else ts
        return int(d.strftime("%Y%m%d"))

    loaded = 0
    inserted_total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i: i + BATCH_SIZE]

        values = []
        for r in batch:
            # fee = 1.4% Stripe standard (démo)
            amount_eur = round(float(r["amount"]) / 100, 2)
            fee_amount = round(amount_eur * 0.014, 4)
            # processing_ms simulé : 20-45ms (démo, cette latence n'est pas mesurée
            # réellement dans le pipeline). Seed = txn_id pour que la valeur soit
            # stable si le MERGE est rejoué (idempotence du chargement).
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
                bool(r.get("fraud_score") is not None and float(r["fraud_score"]) >= 0.85),
                r.get("device_type"),
                processing_ms,
                r["created_at"],
                r.get("merchant_name"), r.get("merchant_email"), r.get("merchant_country_code"),
                r.get("merchant_tier"), r.get("merchant_status"),
                r.get("pm_type"), r.get("pm_brand"),
            ))

        cur.execute(f"TRUNCATE TABLE {STAGING_TABLE}")
        cur.executemany(
            f"INSERT INTO {STAGING_TABLE} VALUES ({', '.join(['%s'] * 22)})", values
        )

        # Dimensions d'abord : les clés de substitution résolues par le MERGE en dépendent.
        upsert_dimensions(cur)

        # MERGE sur txn_id : idempotent en cas de relance du même lot ou du même jour.
        # Clés de substitution résolues par LEFT JOIN dans USING : Snowflake
        # n'accepte pas de sous-requête corrélée dans le VALUES d'un MERGE.
        cur.execute(f"""
            MERGE INTO fact_transactions tgt
            USING (
                SELECT s.*, dm.merchant_key, dc.customer_key, dp.pm_key, dg.geo_key
                FROM {STAGING_TABLE} s
                LEFT JOIN dim_merchant       dm ON dm.merchant_id = s.merchant_id
                LEFT JOIN dim_customer       dc ON dc.customer_id = s.customer_id
                LEFT JOIN dim_payment_method dp ON dp.pm_id       = s.pm_id
                LEFT JOIN dim_geography      dg ON dg.country_code = s.country
            ) src ON tgt.txn_id = src.txn_id
            WHEN NOT MATCHED THEN INSERT (
                txn_id, merchant_key, customer_key, pm_key, date_key, geo_key,
                amount_eur, fee_amount, currency, status, fraud_score, is_fraud,
                device_type, processing_ms, created_at
            )
            VALUES (
                src.txn_id, src.merchant_key, src.customer_key, src.pm_key, src.date_key, src.geo_key,
                src.amount_eur, src.fee_amount, src.currency, src.status, src.fraud_score, src.is_fraud,
                src.device_type, src.processing_ms, src.created_at
            )
        """)
        inserted = cur.fetchone()[0] if cur.rowcount is None else cur.rowcount
        inserted_total += inserted or 0
        loaded += len(batch)
        print(f"  → {loaded}/{len(rows)} lignes traitées ({inserted_total} nouvelles dans fact_transactions)")

    sf.commit()
    cur.close()
    sf.close()
    print(f"[OK] {loaded} transactions chargées dans Snowflake pour {target_date}")


def main():
    """Point d'entrée principal du processus ETL.
    
    Orchestre l'extraction des données depuis PostgreSQL, l'insertion éventuelle des dimensions,
    et le chargement des faits dans Snowflake. Conçu pour être exécuté quotidiennement.
    """
    # Date cible : --date YYYY-MM-DD (le DAG passe {{ ds }}, c'est-à-dire la
    # veille pour une planification quotidienne à 02:00 UTC), sinon aujourd'hui
    # pour un lancement manuel. Sans cet argument, le DAG exportait le jour qui
    # commence — quelques minutes de données — au lieu de la journée écoulée.
    import argparse
    parser = argparse.ArgumentParser(description="Export quotidien PostgreSQL → Snowflake")
    parser.add_argument("--date", help="Jour à exporter (YYYY-MM-DD), défaut : aujourd'hui")
    args = parser.parse_args()
    target = date.fromisoformat(args.date) if args.date else date.today()
    print(f"[START] ETL PostgreSQL → Snowflake — {target}")
    print(f"   Source : {PG_CONFIG['host']}/{PG_CONFIG['dbname']}")
    print(f"   Cible  : {SF_DB}.{SF_SCHEMA}.fact_transactions")
    print()

    print("Extraction depuis PostgreSQL...")
    rows = extract_from_pg(target)
    print(f"  → {len(rows)} transactions succeeded pour {target}")

    if not rows:
        print("[INFO] Aucune transaction à charger — le producer tourne-t-il ?")
        return

    print("Chargement vers Snowflake...")
    load_to_snowflake(rows, target)


if __name__ == "__main__":
    main()
