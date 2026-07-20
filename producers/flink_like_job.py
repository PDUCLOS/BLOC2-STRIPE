#!/usr/bin/env python3
"""
Flink-like job en Python pur — VERSION DEMO LOCALE.

Pour la démo locale, on tourne ce job sur la machine host (venv Python 3.11)
au lieu d'un cluster Flink Docker complet (qui pose des soucis de build
apache-flink/numpy sur ARM64).

Le job fait EXACTEMENT la même chose qu'un job Flink DataStream :
- Source Kafka stripe.public.transactions (CDC)
- Map : scoring fraude avec features Redis
- Sink 1 : Kafka stripe.payments.events
- Sink 2 : Kafka stripe.fraud.alerts

En PROD, on remplace ce script par un vrai job PyFlink submit sur le
JobManager (la logique métier est identique, juste l'API change).
"""
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import redis
from confluent_kafka import Consumer, Producer, KafkaError
import psycopg2
from psycopg2.extras import execute_values

# Load .env (via shared helper à la racine du projet)
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parent.parent if _P(__file__).parent.name != "tests" else _P(__file__).resolve().parent.parent))
import _env  # noqa: F401
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", 5432))
PG_DB = os.environ.get("PG_DB", "stripe_oltp")
PG_USER = os.environ.get("PG_USER", "stripe_app")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "")
FRAUD_THRESHOLD = float(os.environ.get("FRAUD_SCORE_THRESHOLD", 0.85))
REVIEW_THRESHOLD = 0.6

HIGH_RISK_COUNTRIES = {"RU", "NG", "KP", "IR", "VE", "BY"}

_running = True


def signal_handler(sig, frame):
    """Gère l'interruption du script (Ctrl+C ou SIGTERM) pour un arrêt gracieux.
    
    Args:
        sig: Signal reçu.
        frame: Frame courante au moment du signal.
    """
    global _running
    print("\n⏹️  Stopping Flink-like job...")
    _running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def score_transaction(txn, r):
    """Calcule le score de fraude (logique identique au job PyFlink).
    
    Cette fonction s'appuie sur Redis pour stocker et récupérer la "vélocité"
    (nombre de transactions par heure/jour pour un client donné) afin
    d'ajuster le score de fraude en temps réel.
    
    Args:
        txn (dict): Dictionnaire de la transaction provenant de Kafka.
        r (redis.Redis): Instance de connexion à Redis.
        
    Returns:
        dict: La transaction enrichie avec le score, la décision et les règles déclenchées.
              Retourne None si le customer_id est absent.
    """
    customer_id = txn.get("customer_id")
    if not customer_id:
        return None

    txn.pop("__deleted", None)

    # Velocity (Redis)
    velocity_1h = 0
    velocity_24h = 0
    try:
        now_ts = time.time()
        vkey_1h = f"v1h_{customer_id}"
        vkey_24h = f"v24h_{customer_id}"

        # Ajout du timestamp courant dans le sorted set avec le score = timestamp
        r.zadd(vkey_1h, {str(now_ts): now_ts})
        # Suppression des entrées vieilles de plus d'une heure (3600 secondes)
        r.zremrangebyscore(vkey_1h, 0, now_ts - 3600)
        # Expiration de la clé entière pour ne pas polluer Redis si le client devient inactif
        r.expire(vkey_1h, 3700)
        
        # Même logique pour la fenêtre de 24h
        r.zadd(vkey_24h, {str(now_ts): now_ts})
        r.zremrangebyscore(vkey_24h, 0, now_ts - 86400)
        r.expire(vkey_24h, 86500)

        # La cardinalité (zcard) nous donne le nombre de transactions restantes dans la fenêtre
        velocity_1h = r.zcard(vkey_1h)
        velocity_24h = r.zcard(vkey_24h)

        fkey = f"feat_{customer_id}"
        r.hset(fkey, mapping={
            "last_amount": str(txn.get("amount", 0)),
            "last_country": str(txn.get("ip_country", "")),
            "v1h": velocity_1h,
            "v24h": velocity_24h,
        })
        r.expire(fkey, 3600)
    except redis.RedisError as e:
        print(f"  [WARN] Redis: {e}", file=sys.stderr)

    # Score
    # Base à 0.1 pour représenter un risque résiduel minimal sur toute transaction.
    score_val = 0.1
    rules = []

    amount = txn.get("amount", 0) or 0
    country = str(txn.get("ip_country", "") or "")
    device = str(txn.get("device_type", "") or "")

    if amount > 100_000:
        score_val += 0.35
        rules.append("R1_high_amount")
    if 0 < amount < 200 and device in ("pos", "mobile"):
        score_val += 0.15
        rules.append("R2_card_testing")
    if country in HIGH_RISK_COUNTRIES:
        score_val += 0.40
        rules.append("R3_high_risk_geo")
    if velocity_1h > 10:
        score_val += 0.25
        rules.append("R4_velocity_1h")
    if velocity_24h > 50:
        score_val += 0.15
        rules.append("R5_velocity_24h")

    score_val = min(round(score_val, 4), 1.0)

    if score_val >= FRAUD_THRESHOLD:
        decision = "block"
    elif score_val >= REVIEW_THRESHOLD:
        decision = "review"
    else:
        decision = "allow"

    txn["fraud_score"] = score_val
    txn["decision"] = decision
    txn["velocity_1h"] = velocity_1h
    txn["velocity_24h"] = velocity_24h
    txn["rules_triggered"] = rules
    txn["model_version"] = "rule-based-v1"
    txn["scored_at"] = datetime.now(timezone.utc).isoformat()

    return txn


def main():
    """Point d'entrée principal du job Flink-like.
    
    1. Initialise les connexions (Redis, Kafka Consumer/Producer, Postgres).
    2. Lit en continu depuis le topic `stripe.public.transactions` (CDC).
    3. Traite chaque message (scoring).
    4. Pousse le résultat dans `stripe.payments.events` (et `stripe.fraud.alerts` si besoin).
    5. Fait un write-back optionnel du score dans Postgres pour clore la boucle.
    """
    print(f"🚀 Flink-like job started")
    print(f"   Kafka brokers: {KAFKA_BROKERS}")
    print(f"   Redis: {REDIS_HOST}:{REDIS_PORT}")

    # Connexion Redis
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD or None,
        decode_responses=True,
        socket_timeout=2,
    )
    try:
        r.ping()
        print(f"✅ Redis connected")
    except redis.RedisError as e:
        print(f"❌ Redis connection failed: {e}")
        sys.exit(1)

    # Consumer Kafka
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "flink-fraud-scorer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(["stripe.public.transactions"])
    print(f"✅ Kafka consumer subscribed to stripe.public.transactions")

    # Producer Kafka (pour les sinks)
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKERS,
        "linger.ms": 50,
        "compression.type": "none",
    })

    # Connexion Postgres (pour write-back du fraud_score)
    # Note: c'est le même Debezium qui capte cet UPDATE et le re-pousse dans Kafka,
    # créant un cycle maîtrisé via le consumer group "flink-fraud-scorer" qui ignore
    # ses propres messages (auto.offset.reset=earliest au 1er démarrage seulement).
    pg_conn = None
    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD,
            connect_timeout=5,
        )
        print(f"✅ PostgreSQL connected (for fraud_score write-back)")
    except psycopg2.OperationalError as e:
        print(f"⚠️  PostgreSQL not reachable ({e}), fraud_score write-back disabled")

    count = 0
    alert_count = 0
    writeback_count = 0
    start = time.time()

    while _running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"  [ERR] {msg.error()}", file=sys.stderr)
            continue

        # Parse : si KO, on envoie dans le DLQ
        try:
            raw = msg.value()
            if raw is None:
                raise json.JSONDecodeError("Empty message", "", 0)
            txn = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, AttributeError, UnicodeDecodeError) as e:
            dlq_payload = json.dumps({
                "error": f"parse_error: {e}",
                "raw_value_b64": (msg.value() or b"").decode("utf-8", errors="replace")[:500],
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp(),
            }).encode("utf-8")
            producer.produce("stripe.etl.dead-letter", value=dlq_payload)
            producer.poll(0)
            print(f"  [DLQ] parse error at offset {msg.offset()}: {e}", file=sys.stderr)
            count += 1
            continue

        # Scoring : si KO (txn malformé, pas de customer_id, etc), DLQ aussi
        try:
            scored = score_transaction(txn, r)
        except Exception as e:
            dlq_payload = json.dumps({
                "error": f"scoring_error: {e}",
                "txn": txn,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            }).encode("utf-8")
            producer.produce("stripe.etl.dead-letter", value=dlq_payload)
            producer.poll(0)
            print(f"  [DLQ] scoring error at offset {msg.offset()}: {e}", file=sys.stderr)
            count += 1
            continue

        if not scored:
            # Transaction sans customer_id, etc. → on skip silencieusement
            # (c'est un cas valide du CDC, pas une erreur)
            continue

        scored_json = json.dumps(scored)
        # Sink 1 : stripe.payments.events
        producer.produce("stripe.payments.events", value=scored_json.encode("utf-8"))
        # Sink 2 : stripe.fraud.alerts (si decision review/block)
        if scored.get("decision") in ("review", "block"):
            producer.produce("stripe.fraud.alerts", value=scored_json.encode("utf-8"))
            alert_count += 1

        producer.poll(0)  # trigger delivery callbacks
        count += 1

        # Write-back fraud_score dans Postgres (optionnel, mais ferme la boucle)
        # et aligne le dashboard OLTP avec la décision temps réel.
        if pg_conn is not None and scored.get("fraud_score") is not None:
            try:
                with pg_conn.cursor() as cur:
                    cur.execute(
                        """UPDATE transactions
                           SET fraud_score = %s
                           WHERE txn_id = %s AND fraud_score IS NULL""",
                        (scored["fraud_score"], scored["txn_id"]),
                    )
                pg_conn.commit()
                writeback_count += 1
            except psycopg2.Error as e:
                # Si la connexion est morte, on reconnect au prochain message
                print(f"  [WARN] Postgres write-back failed: {e}", file=sys.stderr)
                try:
                    pg_conn.rollback()
                except Exception:
                    pg_conn = None

        if count % 25 == 0:
            elapsed = time.time() - start
            rate = count / elapsed if elapsed > 0 else 0
            print(f"  [{count:6d} txns, {alert_count:4d} alerts, {writeback_count:4d} wb] rate={rate:.1f}/s")

    print(f"✅ Stopped: {count} txns, {alert_count} alerts, {writeback_count} writebacks")
    producer.flush(5)
    consumer.close()
    if pg_conn is not None:
        pg_conn.close()


if __name__ == "__main__":
    main()
