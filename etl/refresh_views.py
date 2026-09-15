#!/usr/bin/env python3
"""Rafraîchit les vues matérialisées Postgres (mv_daily_revenue, mv_merchant_stats).

Créées `WITH NO DATA` dans init/postgres/01_ddl.sql (pour éviter un scan à
vide au premier boot du conteneur) — restent vides tant que personne ne les
rafraîchit explicitement. Ce script est ce "quelqu'un", appelé par le DAG
Airflow (dags/stripe_daily_etl.py) après l'export Snowflake.

Usage : make refresh-views  (ou ./venv/bin/python etl/refresh_views.py)
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

import psycopg2

PG_CONFIG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", 5432)),
    dbname=os.environ.get("PG_DB", "stripe_oltp"),
    user=os.environ.get("PG_USER", "stripe_app"),
    password=os.environ.get("PG_PASSWORD", ""),
)

VIEWS = ["mv_daily_revenue", "mv_merchant_stats"]


def main():
    print("[START] Rafraîchissement des vues matérialisées")
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True
    with conn.cursor() as cur:
        for view in VIEWS:
            # CONCURRENTLY évite de verrouiller les lecteurs pendant le
            # recalcul — nécessite l'index UNIQUE déjà posé sur chaque vue
            # dans 01_ddl.sql (idx_mv_daily_revenue, idx_mv_merchant_stats).
            # Fallback sans CONCURRENTLY au premier refresh : la vue étant
            # WITH NO DATA au départ, un refresh concurrent échoue tant
            # qu'un premier refresh non-concurrent ne l'a pas peuplée.
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
            except psycopg2.errors.FeatureNotSupported:
                # "CONCURRENTLY cannot be used when the materialized view is
                # not populated" — cas du tout premier refresh après un
                # WITH NO DATA. Les refreshs suivants utiliseront CONCURRENTLY.
                cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
            print(f"  [OK] {view} rafraîchie")
    conn.close()
    print("[OK] Vues matérialisées à jour")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Erreur refresh vues : {e}", file=sys.stderr)
        sys.exit(1)
