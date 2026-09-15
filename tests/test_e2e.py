#!/usr/bin/env python3
"""
Tests E2E — Architecture polyglotte Stripe.
Valide le pipeline complet : PostgreSQL → Kafka CDC → Redis → MongoDB.

Usage : make test  (ou ./venv/bin/python tests/test_e2e.py)
"""
import os
import sys
import time
import uuid
import json
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

import psycopg2
from psycopg2.extras import RealDictCursor

# ─── Config ────────────────────────────────────────────────────────────────────
PG_CONFIG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", 5432)),
    dbname=os.environ.get("PG_DB", "stripe_oltp"),
    user=os.environ.get("PG_USER", "stripe_app"),
    password=os.environ.get("PG_PASSWORD", ""),
)
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None
# Construit depuis MONGO_APP_USER/MONGO_APP_PASSWORD (mêmes noms que
# init/mongo/02_app_user.js et scripts/init_env.sh) plutôt qu'un URI en dur :
# make init-env génère un MONGO_APP_PASSWORD aléatoire par installation, donc
# un mot de passe codé en dur ici ne correspondrait jamais à la vraie base.
MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_APP_USER = os.environ.get("MONGO_APP_USER", "stripe_app")
MONGO_APP_PASSWORD = os.environ.get("MONGO_APP_PASSWORD", "stripe_app_dev")
MONGO_DB = os.environ.get("MONGO_DB", "stripe_nosql")
MONGO_URI = os.environ.get(
    "MONGO_URI",
    f"mongodb://{MONGO_APP_USER}:{MONGO_APP_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin",
)

FRAUD_THRESHOLD  = float(os.environ.get("FRAUD_THRESHOLD", 0.85))
REVIEW_THRESHOLD = float(os.environ.get("REVIEW_THRESHOLD", 0.60))

PASS = "[OK]"
FAIL = "[FAIL]"
SKIP = "[SKIP]"

results = []


def check(name, fn):
    """Execute test fn, record pass/fail."""
    try:
        fn()
        results.append((PASS, name))
        print(f"{PASS} {name}")
    except AssertionError as e:
        results.append((FAIL, name, str(e)))
        print(f"{FAIL} {name}  →  {e}")
    except Exception as e:
        results.append((FAIL, name, str(e)))
        print(f"{FAIL} {name}  →  {type(e).__name__}: {e}")


# Convention de test: chaque test doit rester autonome (setup/cleanup local)
# pour éviter les effets de bord entre validations successives.


# ─── PostgreSQL ─────────────────────────────────────────────────────────────────
def test_pg_connect():
    """Vérifie la connexion de base à PostgreSQL."""
    conn = psycopg2.connect(**PG_CONFIG)
    conn.close()


def test_pg_schema():
    """Vérifie que les 6 tables principales existent dans le schéma public."""
    conn = psycopg2.connect(**PG_CONFIG)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename IN (
                'merchants','customers','payment_methods',
                'transactions','refunds','fraud_indicators'
              )
        """)
        tables = {r[0] for r in cur.fetchall()}
    conn.close()
    expected = {"merchants","customers","payment_methods","transactions","refunds","fraud_indicators"}
    missing = expected - tables
    assert not missing, f"Tables manquantes : {missing}"


def test_pg_seed_data():
    """Vérifie qu'il y a suffisamment de données générées par le seed (marchands et clients)."""
    conn = psycopg2.connect(**PG_CONFIG)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM merchants")
        merchants = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM customers")
        customers = cur.fetchone()[0]
    conn.close()
    assert merchants >= 100, f"Pas assez de marchands : {merchants} (seed KO ?)"
    assert customers >= 1000, f"Pas assez de clients : {customers} (seed KO ?)"


def test_pg_transaction_insert():
    """Insère une transaction test et vérifie qu'elle est bien persistée."""
    conn = psycopg2.connect(**PG_CONFIG)
    idempotency = f"test-e2e-{uuid.uuid4()}"
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Récupère un merchant et un customer existants
            cur.execute("SELECT merchant_id FROM merchants LIMIT 1")
            merchant_id = cur.fetchone()["merchant_id"]
            cur.execute("SELECT customer_id FROM customers LIMIT 1")
            customer_id = cur.fetchone()["customer_id"]
            cur.execute("SELECT pm_id FROM payment_methods WHERE customer_id = %s LIMIT 1", (customer_id,))
            row = cur.fetchone()
            pm_id = row["pm_id"] if row else None

            cur.execute("""
                INSERT INTO transactions
                    (merchant_id, customer_id, pm_id, amount, currency,
                     status, idempotency_key, device_type, ip_country)
                VALUES (%s, %s, %s, 9999, 'EUR', 'succeeded', %s, 'web', 'FR')
                RETURNING txn_id
            """, (merchant_id, customer_id, pm_id, idempotency))
            txn_id = cur.fetchone()["txn_id"]
        conn.commit()

        # Vérifie relecture
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM transactions WHERE txn_id = %s", (txn_id,))
            row = cur.fetchone()
        assert row is not None, "Transaction introuvable après insertion"
        assert row["amount"] == 9999, f"Montant incorrect : {row['amount']}"

        # Nettoyage
        with conn.cursor() as cur:
            cur.execute("DELETE FROM transactions WHERE txn_id = %s", (txn_id,))
        conn.commit()
    finally:
        conn.close()


def test_pg_idempotency():
    """Double insertion avec même idempotency_key → unique constraint."""
    import psycopg2.errors
    conn = psycopg2.connect(**PG_CONFIG)
    idem = f"test-idem-{uuid.uuid4()}"
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT merchant_id FROM merchants LIMIT 1")
            mid = cur.fetchone()["merchant_id"]
        conn.commit()

        def insert():
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO transactions (merchant_id, amount, currency, status, idempotency_key)
                    VALUES (%s, 100, 'EUR', 'succeeded', %s)
                """, (mid, idem))
            conn.commit()

        insert()
        raised = False
        try:
            insert()
        except psycopg2.errors.UniqueViolation:
            raised = True
            conn.rollback()
        assert raised, "Unique constraint sur idempotency_key non déclenché"

        with conn.cursor() as cur:
            cur.execute("DELETE FROM transactions WHERE idempotency_key = %s", (idem,))
        conn.commit()
    finally:
        conn.close()


def test_pg_debezium_publication():
    """Publication Debezium stripe_publication doit exister."""
    conn = psycopg2.connect(**PG_CONFIG)
    with conn.cursor() as cur:
        cur.execute("SELECT pubname FROM pg_publication WHERE pubname = 'stripe_publication'")
        row = cur.fetchone()
    conn.close()
    assert row is not None, "Publication stripe_publication introuvable — Debezium CDC KO"


def test_pg_materialized_views():
    """Vérifie la présence ET le contenu des vues matérialisées.

    Une vue peut exister (CREATE MATERIALIZED VIEW a tourné une fois) sans
    jamais avoir été rafraîchie depuis — vérifier seulement pg_matviews donne
    un faux sentiment de couverture si etl/refresh_views.py n'a jamais
    réellement tourné. On vérifie donc aussi qu'elles contiennent des lignes.
    """
    conn = psycopg2.connect(**PG_CONFIG)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT matviewname FROM pg_matviews
            WHERE matviewname IN ('mv_daily_revenue','mv_merchant_stats')
        """)
        views = {r[0] for r in cur.fetchall()}
        assert "mv_daily_revenue" in views, "mv_daily_revenue manquante"
        assert "mv_merchant_stats" in views, "mv_merchant_stats manquante"

        cur.execute("SELECT COUNT(*) FROM mv_daily_revenue")
        n_revenue = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM mv_merchant_stats")
        n_merchant = cur.fetchone()[0]
    conn.close()
    assert n_revenue > 0, "mv_daily_revenue existe mais est vide — jamais rafraîchie ? (make refresh-views)"
    assert n_merchant > 0, "mv_merchant_stats existe mais est vide — jamais rafraîchie ? (make refresh-views)"


def test_pg_amount_bigint():
    """Vérifie que amount est bien BIGINT (pas FLOAT).

    Les montants sont stockés en centimes (entiers) pour éviter les erreurs
    d'arrondi propres aux flottants sur des calculs financiers — un FLOAT
    introduirait un risque de dérive centime par centime sur les agrégats.
    """
    conn = psycopg2.connect(**PG_CONFIG)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_name = 'transactions' AND column_name = 'amount'
        """)
        row = cur.fetchone()
    conn.close()
    assert row is not None, "Colonne amount introuvable"
    assert row[0] == "bigint", f"Type incorrect pour amount : {row[0]} (attendu bigint)"


# ─── Redis ──────────────────────────────────────────────────────────────────────
def test_redis_connect():
    """Vérifie la connexion de base à Redis via un PING."""
    import redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    assert r.ping(), "Redis PING KO"
    r.close()


def test_redis_velocity_write_read():
    """Simule l'écriture d'une vélocité et la relit."""
    import redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    test_customer = f"test-cust-{uuid.uuid4().hex[:8]}"
    now_ts = time.time()

    # Écriture ZSET (même pattern que flink_like_job.py)
    r.zadd(f"v1h_{test_customer}", {str(now_ts): now_ts})
    r.expire(f"v1h_{test_customer}", 3700)

    # Lecture
    cutoff = now_ts - 3600
    count = r.zcount(f"v1h_{test_customer}", cutoff, "+inf")
    assert count == 1, f"Vélocité 1h incorrecte : {count}"

    # Nettoyage
    r.delete(f"v1h_{test_customer}")
    r.close()


def test_redis_feature_store():
    """Vérifie le pattern HSET features."""
    import redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    key = f"features:test-{uuid.uuid4().hex[:8]}"

    r.hset(key, mapping={
        "fraud_score": "0.12",
        "velocity_1h": "3",
        "velocity_24h": "15",
    })
    r.expire(key, 60)

    val = r.hgetall(key)
    assert val["fraud_score"] == "0.12", f"Feature KO : {val}"

    r.delete(key)
    r.close()


# ─── MongoDB ────────────────────────────────────────────────────────────────────
def test_mongo_connect():
    """Vérifie la connexion de base à MongoDB via une commande ping admin."""
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    client.close()


def test_mongo_collections():
    """Vérifie que les collections principales existent (créées à la volée par le writer)."""
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client["stripe_nosql"]
    existing = set(db.list_collection_names())
    expected = {"transaction_logs", "fraud_alerts"}
    missing = expected - existing
    client.close()
    # Assert volontairement "souple" : en environnement froid, les collections
    # peuvent ne pas exister tant que le writer n'a pas encore consommé de messages.
    if missing:
        raise AssertionError(f"Collections absentes (le pipeline tourne-t-il ?) : {missing}")


def test_mongo_fraud_alerts_schema():
    """Vérifie la structure d'une fraud_alert si elle existe."""
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client["stripe_nosql"]
    doc = db["fraud_alerts"].find_one()
    client.close()
    if doc is None:
        # Pas d'alerte encore — pas bloquant
        return
    # "created_at", pas "timestamp" — nom du champ tel qu'écrit par mongo_writer.py.
    for field in ["txn_id", "fraud_score", "decision", "created_at"]:
        assert field in doc, f"Champ '{field}' absent de fraud_alerts"


def test_mongo_ttl_indexes():
    """Vérifie que les index TTL RGPD sont en place."""
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client["stripe_nosql"]
    ttl_collections = {"transaction_logs": 90, "user_interactions": 30}
    issues = []
    for coll, days in ttl_collections.items():
        indexes = list(db[coll].list_indexes())
        has_ttl = any(idx.get("expireAfterSeconds") is not None for idx in indexes)
        if not has_ttl:
            issues.append(f"{coll} manque TTL index")
    client.close()
    if issues:
        raise AssertionError(" | ".join(issues))


# ─── Pipeline end-to-end (optionnel — nécessite pipeline actif) ─────────────────
def test_pipeline_fraud_scoring_high_amount():
    """
    Insère une transaction à montant élevé (>100k centimes = fraude probable)
    et vérifie que le fraud_score remonte dans PostgreSQL après ~5s.
    Nécessite flink_like_job.py actif.
    """
    conn = psycopg2.connect(**PG_CONFIG)
    idem = f"test-fraud-{uuid.uuid4()}"
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT merchant_id FROM merchants LIMIT 1")
            mid = cur.fetchone()["merchant_id"]
            cur.execute("SELECT customer_id FROM customers LIMIT 1")
            cid = cur.fetchone()["customer_id"]
            cur.execute("SELECT pm_id FROM payment_methods WHERE customer_id = %s LIMIT 1", (cid,))
            row = cur.fetchone()
            pm_id = row["pm_id"] if row else None

            cur.execute("""
                INSERT INTO transactions
                    (merchant_id, customer_id, pm_id, amount, currency,
                     status, idempotency_key, device_type, ip_country)
                VALUES (%s, %s, %s, 150000, 'EUR', 'succeeded', %s, 'web', 'RU')
                RETURNING txn_id
            """, (mid, cid, pm_id, idem))
            txn_id = cur.fetchone()["txn_id"]
        conn.commit()

        # Attente du scoring (max 10s)
        scored = False
        for _ in range(10):
            time.sleep(1)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT fraud_score FROM transactions WHERE txn_id = %s", (txn_id,))
                row = cur.fetchone()
            if row and row["fraud_score"] is not None and float(row["fraud_score"]) > 0:
                scored = True
                break

        if not scored:
            raise AssertionError(
                "fraud_score non mis à jour après 10s — flink_like_job.py tourne-t-il ?"
            )

        # Nettoyage
        with conn.cursor() as cur:
            cur.execute("DELETE FROM transactions WHERE txn_id = %s", (txn_id,))
        conn.commit()
    finally:
        conn.close()


# ─── Runner ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Tests E2E — Architecture polyglotte Stripe")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    # PostgreSQL
    print("\n── PostgreSQL ──")
    check("Connexion PostgreSQL", test_pg_connect)
    check("Schema 6 tables", test_pg_schema)
    check("Données seed (merchants + customers)", test_pg_seed_data)
    check("Insert/read transaction", test_pg_transaction_insert)
    check("Idempotency key unique constraint", test_pg_idempotency)
    check("Publication Debezium stripe_publication", test_pg_debezium_publication)
    check("Vues matérialisées", test_pg_materialized_views)
    check("Colonne amount = BIGINT (pas FLOAT)", test_pg_amount_bigint)

    # Redis
    print("\n── Redis ──")
    check("Connexion Redis", test_redis_connect)
    check("Velocity ZSET write/read", test_redis_velocity_write_read)
    check("Feature store HSET", test_redis_feature_store)

    # MongoDB
    print("\n── MongoDB ──")
    check("Connexion MongoDB", test_mongo_connect)
    check("Collections transaction_logs + fraud_alerts", test_mongo_collections)
    check("Schema fraud_alerts", test_mongo_fraud_alerts_schema)
    check("Index TTL RGPD", test_mongo_ttl_indexes)

    # Pipeline E2E
    print("\n── Pipeline E2E (nécessite pipeline actif) ──")
    check("Scoring fraude — montant élevé (>100k centimes)", test_pipeline_fraud_scoring_high_amount)

    # Résumé
    passed = sum(1 for r in results if r[0] == PASS)
    failed = sum(1 for r in results if r[0] == FAIL)
    total  = len(results)

    print("\n" + "=" * 60)
    print(f"  Résultats : {passed}/{total} tests passés")
    if failed:
        print(f"  [FAIL] {failed} échec(s) :")
        for r in results:
            if r[0] == FAIL:
                print(f"     - {r[1]} → {r[2] if len(r) > 2 else ''}")
    else:
        print("  [OK] Tous les tests sont au vert")
    print("=" * 60)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
