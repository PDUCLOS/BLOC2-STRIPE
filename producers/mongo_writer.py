#!/usr/bin/env python3
"""Kafka → MongoDB consumer.

Lit le topic stripe.payments.events (transactions scorées par Flink)
et les écrit dans :
  - transaction_logs (toutes les transactions)
  - fraud_alerts (celles avec decision in [review, block])
"""
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Load .env (via shared helper à la racine du projet)
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parent.parent if _P(__file__).parent.name != "tests" else _P(__file__).resolve().parent.parent))
import _env  # noqa: F401
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_USER = os.environ.get("MONGO_USER", "admin")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD", "")
MONGO_DB = os.environ.get("MONGO_DB", "stripe_nosql")

_running = True


def signal_handler(sig, frame):
    """Ctrl+C / SIGTERM -> on laisse la boucle finir son tour au lieu de couper en plein commit."""
    global _running
    print("\n[STOP] Stopping Mongo writer...")
    _running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_mongo_client():
    """Initialise et retourne un client MongoDB en utilisant les variables d'environnement.
    
    Returns:
        MongoClient: Instance du client PyMongo connecté.
    """
    if MONGO_USER and MONGO_PASSWORD:
        uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    else:
        uri = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def main():
    """Consomme stripe.payments.events, écrit transaction_logs (+ fraud_alerts si review/block). Les erreurs de parsing/insertion partent en DLQ plutôt que de faire planter la boucle."""
    print(f"[START] Mongo writer started: Kafka {KAFKA_BROKERS} → Mongo {MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}")

    mongo_client = get_mongo_client()
    db = mongo_client[MONGO_DB]

    # Ping to verify connection
    try:
        mongo_client.admin.command("ping")
        print("[OK] Connected to MongoDB")
    except PyMongoError as e:
        print(f"[ERROR] MongoDB connection failed: {e}")
        sys.exit(1)

    # Producer Kafka pour DLQ
    from kafka import KafkaProducer
    dlq_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS.split(","),
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
    )

    consumer = KafkaConsumer(
        "stripe.payments.events",
        bootstrap_servers=KAFKA_BROKERS.split(","),
        group_id="mongo-writer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8") if v else None,
    )
    # Ce consumer lit le flux scoré canonique; les alertes sont dérivées localement
    # pour garder une source unique de vérité applicative.

    count = 0
    alert_count = 0
    dlq_count = 0
    for msg in consumer:
        if not _running:
            break

        # Parse JSON
        try:
            txn = json.loads(msg.value)
        except (json.JSONDecodeError, TypeError) as e:
            # Envoi vers le Dead Letter Queue (DLQ) en cas de message malformé
            dlq_producer.send("stripe.etl.dead-letter", value={
                "error": f"parse_error: {e}",
                "raw_value": (msg.value or "")[:500],
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
            })
            dlq_count += 1
            print(f"  [DLQ] parse error at offset {msg.offset}: {e}", file=sys.stderr)
            continue

        # Write to Mongo
        try:
            now = datetime.now(timezone.utc)

            # transaction_logs — TTL 90 jours RGPD (toutes les transactions)
            db.transaction_logs.insert_one({
                "txn_id": txn.get("txn_id"),
                "event_type": "transaction.scored",
                "payload": txn,
                "source": "mongo-writer",
                "created_at": now,
            })

            # logs — monitoring opérationnel (requêtes stripe_queries_nosql.js Section 3)
            # Collection séparée pour distinguer les traces techniques des données métier.
            db.logs.insert_one({
                "service": "mongo-writer",
                "type": "access",
                "level": "INFO",
                "message": f"transaction.scored txn_id={txn.get('txn_id')}",
                "context": {
                    "txn_id": txn.get("txn_id"),
                    "decision": txn.get("decision"),
                    "duration_ms": round(5 + (txn.get("fraud_score") or 0) * 10, 1),
                },
                "created_at": now,
                "ttl_expires_at": datetime.fromtimestamp(
                    now.timestamp() + 90 * 86400, tz=timezone.utc
                ),
            })

            count += 1

            # Si alerte, on écrit dans fraud_alerts
            if txn.get("decision") in ("review", "block"):
                db.fraud_alerts.insert_one({
                    "txn_id": txn.get("txn_id"),
                    "customer_id": txn.get("customer_id"),
                    "merchant_id": txn.get("merchant_id"),
                    "amount": txn.get("amount"),
                    "currency": txn.get("currency"),
                    "ip_country": txn.get("ip_country"),
                    "device_type": txn.get("device_type"),
                    "fraud_score": txn.get("fraud_score"),
                    "decision": txn.get("decision"),
                    "rules_triggered": txn.get("rules_triggered"),
                    "model_version": txn.get("model_version"),
                    "velocity_1h": txn.get("velocity_1h"),
                    "velocity_24h": txn.get("velocity_24h"),
                    "created_at": datetime.now(timezone.utc),
                })
                alert_count += 1

        except PyMongoError as e:
            dlq_producer.send("stripe.etl.dead-letter", value={
                "error": f"mongo_write_error: {e}",
                "txn": txn,
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
            })
            dlq_count += 1
            print(f"  [DLQ] Mongo write failed for txn {txn.get('txn_id')}: {e}", file=sys.stderr)

        if count % 50 == 0:
            print(f"  [{count:6d} txns, {alert_count:4d} alerts, {dlq_count:3d} dlq] processed")

    print(f"[OK] Mongo writer stopped: {count} txns, {alert_count} alerts, {dlq_count} dlq")
    consumer.close()
    dlq_producer.flush(5)
    dlq_producer.close()
    mongo_client.close()


if __name__ == "__main__":
    main()
