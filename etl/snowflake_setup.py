#!/usr/bin/env python3
"""
Crée le warehouse, la DB, le schéma et les tables Snowflake (star schema OLAP).
Idempotent — safe to re-run.

Usage : make snowflake-setup  (ou ./venv/bin/python etl/snowflake_setup.py)
Prérequis : variables SNOWFLAKE_* dans .env
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

REQUIRED = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
missing = [v for v in REQUIRED if not os.environ.get(v)]
if missing:
    print(f"[ERROR] Variables manquantes dans .env : {', '.join(missing)}")
    print("   Crée un compte trial sur https://signup.snowflake.com/ et remplis .env")
    sys.exit(1)

try:
    import snowflake.connector
except ImportError:
    print("[ERROR] snowflake-connector-python manquant — pip install snowflake-connector-python")
    sys.exit(1)

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "STRIPE_WH")
DATABASE  = os.environ.get("SNOWFLAKE_DB", "STRIPE_DWH")
SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA", "PROD")


def run(cur, sql, label=""):
    """Exécute une requête SQL, affiche un label de statut, et ignore les erreurs de type "déjà existant".
    
    Args:
        cur: Curseur Snowflake.
        sql (str): Requête SQL à exécuter.
        label (str, optional): Message à afficher dans la console en cas de succès.
    """
    try:
        # Centraliser l'exécution ici garantit un logging homogène de toutes les étapes DDL.
        cur.execute(sql)
        if label:
            print(f"  [OK] {label}")
    except snowflake.connector.errors.ProgrammingError as e:
        if "already exists" in str(e).lower():
            if label:
                print(f"  [SKIP] {label} (existe déjà)")
        else:
            raise


def main():
    """Crée warehouse + DB + schéma + tables (star schema), puis pré-remplit dim_date/dim_geography. Idempotent, safe à relancer."""
    print(f"Connexion à Snowflake ({ACCOUNT})...")
    conn = snowflake.connector.connect(
        account=ACCOUNT, user=USER, password=PASSWORD,
    )
    cur = conn.cursor()
    print("[OK] Connecté")

    print("\nCréation du warehouse...")
    run(cur, f"""
        CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE}
            WAREHOUSE_SIZE = 'X-SMALL'
            AUTO_SUSPEND = 60
            AUTO_RESUME = TRUE
            COMMENT = 'Stripe Polyglot — démo RNCP41993'
    """, f"Warehouse {WAREHOUSE}")

    cur.execute(f"USE WAREHOUSE {WAREHOUSE}")

    print("\nCréation de la base de données...")
    run(cur, f"CREATE DATABASE IF NOT EXISTS {DATABASE}", f"Database {DATABASE}")
    cur.execute(f"USE DATABASE {DATABASE}")

    run(cur, f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}", f"Schema {SCHEMA}")
    cur.execute(f"USE SCHEMA {SCHEMA}")

    print("\nCréation des dimensions...")
    # Dimensions séparées pour garder des faits compacts et faciliter les agrégations BI.

    run(cur, """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key    NUMBER        NOT NULL PRIMARY KEY,
            full_date   DATE          NOT NULL,
            year        NUMBER(4)     NOT NULL,
            quarter     NUMBER(1)     NOT NULL,
            month       NUMBER(2)     NOT NULL,
            week        NUMBER(2)     NOT NULL,
            day_of_week NUMBER(1)     NOT NULL,
            is_weekend  BOOLEAN       NOT NULL
        )
    """, "Table dim_date")

    run(cur, """
        CREATE TABLE IF NOT EXISTS dim_merchant (
            merchant_key  NUMBER AUTOINCREMENT PRIMARY KEY,
            merchant_id   VARCHAR(36)  NOT NULL UNIQUE,
            name          VARCHAR(255),
            email         VARCHAR(255),
            country       CHAR(2),
            tier          VARCHAR(20),
            status        VARCHAR(20),
            valid_from    TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            is_current    BOOLEAN      DEFAULT TRUE
        )
    """, "Table dim_merchant")

    run(cur, """
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_key  NUMBER AUTOINCREMENT PRIMARY KEY,
            customer_id   VARCHAR(36)  NOT NULL UNIQUE,
            segment       VARCHAR(50),
            country       CHAR(2),
            valid_from    TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            is_current    BOOLEAN      DEFAULT TRUE
        )
    """, "Table dim_customer")

    run(cur, """
        CREATE TABLE IF NOT EXISTS dim_payment_method (
            pm_key          NUMBER AUTOINCREMENT PRIMARY KEY,
            pm_id           VARCHAR(36) UNIQUE,
            type            VARCHAR(30),
            brand           VARCHAR(20),
            is_digital_wallet BOOLEAN DEFAULT FALSE
        )
    """, "Table dim_payment_method")

    run(cur, """
        CREATE TABLE IF NOT EXISTS dim_geography (
            geo_key      NUMBER AUTOINCREMENT PRIMARY KEY,
            country_code CHAR(2)       UNIQUE,
            country_name VARCHAR(100),
            region       VARCHAR(50),
            is_high_risk BOOLEAN       DEFAULT FALSE
        )
    """, "Table dim_geography")

    print("\nCréation de la table de faits...")
    run(cur, f"""
        CREATE TABLE IF NOT EXISTS fact_transactions (
            txn_key        NUMBER AUTOINCREMENT  NOT NULL PRIMARY KEY,
            txn_id         VARCHAR(36)           NOT NULL UNIQUE,
            merchant_key   NUMBER                REFERENCES dim_merchant(merchant_key),
            customer_key   NUMBER                REFERENCES dim_customer(customer_key),
            pm_key         NUMBER                REFERENCES dim_payment_method(pm_key),
            date_key       NUMBER                REFERENCES dim_date(date_key),
            geo_key        NUMBER                REFERENCES dim_geography(geo_key),
            amount_eur     NUMBER(18,2)          NOT NULL,
            fee_amount     NUMBER(18,2),
            currency       CHAR(3),
            status         VARCHAR(20),
            fraud_score    NUMBER(5,4),
            is_fraud       BOOLEAN,
            device_type    VARCHAR(50),
            processing_ms  NUMBER(8,2),
            created_at     TIMESTAMP_TZ
        )
        CLUSTER BY (date_key, merchant_key)
    """, "Table fact_transactions")

    # Le clustering date+merchant accélère les filtres temporels et les top marchands.

    print("\nPré-peuplement de dim_date (2020-2030)...")
    cur.execute("SELECT COUNT(*) FROM dim_date")
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute("""
            INSERT INTO dim_date
            SELECT
                TO_NUMBER(TO_CHAR(d::DATE, 'YYYYMMDD'))       AS date_key,
                d::DATE                                        AS full_date,
                YEAR(d)                                        AS year,
                QUARTER(d)                                     AS quarter,
                MONTH(d)                                       AS month,
                WEEKOFYEAR(d)                                  AS week,
                DAYOFWEEK(d)                                   AS day_of_week,
                DAYOFWEEK(d) IN (0, 6)                         AS is_weekend
            FROM (
                SELECT DATEADD('day', SEQ4(), '2020-01-01')    AS d
                FROM TABLE(GENERATOR(ROWCOUNT => 3653))
            )
        """)
        print("  [OK] dim_date peuplée (2020→2029)")
    else:
        print(f"  [SKIP] dim_date existe déjà ({count} lignes)")

    print("\nPeuplement de dim_geography...")
    cur.execute("SELECT COUNT(*) FROM dim_geography")
    if cur.fetchone()[0] == 0:
        geo_data = [
            ("FR", "France", "Europe", False),
            ("US", "United States", "Americas", False),
            ("GB", "United Kingdom", "Europe", False),
            ("DE", "Germany", "Europe", False),
            ("ES", "Spain", "Europe", False),
            ("IT", "Italy", "Europe", False),
            ("NL", "Netherlands", "Europe", False),
            ("BE", "Belgium", "Europe", False),
            ("CA", "Canada", "Americas", False),
            ("JP", "Japan", "Asia", False),
            ("AU", "Australia", "Oceania", False),
            ("BR", "Brazil", "Americas", False),
            ("RU", "Russia", "Europe/Asia", True),
            ("NG", "Nigeria", "Africa", True),
            ("KP", "North Korea", "Asia", True),
            ("IR", "Iran", "Asia", True),
            ("VE", "Venezuela", "Americas", True),
        ]
        cur.executemany(
            "INSERT INTO dim_geography (country_code, country_name, region, is_high_risk) VALUES (%s, %s, %s, %s)",
            geo_data
        )
        print(f"  [OK] dim_geography peuplée ({len(geo_data)} pays)")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n[OK] Snowflake setup terminé !")
    print(f"   Warehouse : {WAREHOUSE}")
    print(f"   Database  : {DATABASE}.{SCHEMA}")
    print(f"   Tables    : dim_date, dim_merchant, dim_customer, dim_payment_method, dim_geography, fact_transactions")
    print(f"\n   Prochaine étape : make snowflake-export")


if __name__ == "__main__":
    main()
