"""Rejoue queries/snowflake_olap.sql sur le compte Snowflake configuré.

Statement par statement (découpage sur « ; » hors commentaires), affiche les
premières lignes de chaque résultat et sort en erreur au premier statement
qui échoue : une requête désalignée du schéma réel fait échouer la cible
`make snowflake-check`. Sans SNOWFLAKE_ACCOUNT (cas de la CI), sortie 0 avec
un message : la vérification est sautée, pas simulée.
Usage : make snowflake-check
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

QUERIES = Path(__file__).resolve().parent.parent / "queries" / "snowflake_olap.sql"


def statements(sql_text):
    body = "\n".join(line for line in sql_text.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in body.split(";") if s.strip()]


def main():
    account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
    if not account:
        print("[SKIP] SNOWFLAKE_ACCOUNT non défini : requêtes Snowflake non vérifiées (compte d'essai en local uniquement)")
        return 0
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=account,
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "STRIPE_WH"),
        database=os.environ.get("SNOWFLAKE_DB", "STRIPE_DWH"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PROD"),
    )
    cur = conn.cursor()
    failed = 0
    for k, stmt in enumerate(statements(QUERIES.read_text(encoding="utf-8")), 1):
        head = " ".join(stmt.split())[:80]
        try:
            cur.execute(stmt)
            rows = cur.fetchmany(3) if cur.description else []
            print(f"[OK] {k}. {head}")
            for row in rows:
                print("      ", row)
        except Exception as exc:  # affiche et continue pour lister toutes les erreurs
            failed += 1
            print(f"[FAIL] {k}. {head}\n       {str(exc)[:200]}")
    conn.close()
    if failed:
        print(f"[ERROR] {failed} statement(s) en échec")
        return 1
    print("[OK] Toutes les requêtes Snowflake s'exécutent sur le compte")
    return 0


if __name__ == "__main__":
    sys.exit(main())
