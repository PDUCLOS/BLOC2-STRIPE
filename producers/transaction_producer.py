#!/usr/bin/env python3
"""Transaction producer — generates a continuous stream of transactions.

Mix of legitimate (95%) and fraudulent (5%) patterns. Inserts directly
into PostgreSQL. The CDC pipeline (Debezium → Kafka) picks them up and
propagates to the rest of the stack.

Usage:
    python producers/transaction_producer.py           # 5 txn/s, 5% fraud
    python producers/transaction_producer.py --rate 10 # 10 txn/s
"""
import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

# Load .env (via shared helper à la racine du projet)
import sys
from pathlib import Path as _P
sys.path.insert(0, str(_P(__file__).resolve().parent.parent if _P(__file__).parent.name != "tests" else _P(__file__).resolve().parent.parent))
import _env  # noqa: F401
PG_CONFIG = {
    "host": os.environ["PG_HOST"],
    "port": int(os.environ.get("PG_PORT", 5432)),
    "dbname": os.environ["PG_DB"],
    "user": os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
}

DEVICE_TYPES = ["mobile", "desktop", "tablet", "pos"]
CURRENCIES = ["EUR", "USD", "GBP", "JPY", "CAD", "AUD"]
FRAUD_COUNTRIES = ["RU", "NG", "KP", "IR", "VE"]  # high-risk geo
NORMAL_COUNTRIES = ["FR", "US", "GB", "DE", "ES", "IT", "NL", "BE", "CA"]

_running = True


def signal_handler(sig, frame):
    """Ctrl+C / SIGTERM : on stoppe après l'itération en cours pour ne pas laisser une transaction à moitié insérée."""
    global _running
    print("\n[STOP] Stopping producer...")
    _running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def pick_merchant(cur):
    """Sélectionne un marchand actif de manière aléatoire.
    
    Args:
        cur: Curseur de base de données PostgreSQL.
        
    Returns:
        L'identifiant du marchand sélectionné (merchant_id).
    """
    cur.execute("SELECT merchant_id FROM merchants WHERE status = 'active' ORDER BY random() LIMIT 1")
    return cur.fetchone()[0]


def pick_customer_pm(cur, is_fraud: bool):
    """Sélectionne un client et son moyen de paiement associé.
    
    Les cas de fraude ciblent les comptes récents ou inactifs, simulant un comportement
    typique d'attaquants utilisant des comptes compromis ou nouvellement créés.
    
    Args:
        cur: Curseur de base de données PostgreSQL.
        is_fraud (bool): Indique s'il faut sélectionner un profil pour une fraude.
        
    Returns:
        tuple: (customer_id, pm_id) ou (None, None) si aucun trouvé.
    """
    if is_fraud:
        # Cible : nouveaux comptes ou segments inactifs (profil typique de fraude)
        cur.execute("""
            SELECT c.customer_id, pm.pm_id
            FROM customers c
            JOIN payment_methods pm ON pm.customer_id = c.customer_id
            WHERE c.segment IN ('new', 'inactive')
            ORDER BY random() LIMIT 1
        """)
    else:
        cur.execute("""
            SELECT c.customer_id, pm.pm_id
            FROM customers c
            JOIN payment_methods pm ON pm.customer_id = c.customer_id
            WHERE c.segment IN ('standard', 'premium')
            ORDER BY random() LIMIT 1
        """)
    row = cur.fetchone()
    return row if row else (None, None)


def build_transaction(is_fraud: bool) -> dict:
    """Build a transaction payload. Fraud patterns:
       - gros montant (>500€) + pays à risque + device mobile inconnu
       - ou petit montant répété (card testing) signalé via metadata
    """
    amount = random.randint(50, 50000)  # centimes, 0.50€ → 500€
    if is_fraud:
        # 70% gros montant, 30% petit (card testing)
        if random.random() < 0.7:
            amount = random.randint(10000, 200000)  # 100€ → 2000€
        else:
            amount = random.randint(100, 500)  # card testing : 1€ → 5€
    currency = random.choice(CURRENCIES)

    # Répartition volontaire pour produire un dataset mixte (fraude vs normal)
    # qui reste exploitable visuellement dans le dashboard.
    if is_fraud and random.random() < 0.5:
        ip_country = random.choice(FRAUD_COUNTRIES)
    else:
        ip_country = random.choice(NORMAL_COUNTRIES)

    device = random.choice(DEVICE_TYPES)
    if is_fraud and random.random() < 0.3:
        device = "pos"  # atypical device for online fraud

    return {
        "merchant_id": None,  # filled in main()
        "customer_id": None,
        "pm_id": None,
        "amount": amount,
        "currency": currency,
        "status": "succeeded",
        "fraud_score": None,  # filled by Flink
        "idempotency_key": str(uuid.uuid4()),
        "device_type": device,
        "ip_country": ip_country,
        "metadata": json.dumps({
            "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36" if not is_fraud else "curl/8.4.0",
            "session_id": str(uuid.uuid4()),
            "is_fraud_pattern": is_fraud,
            "producer_ts": datetime.now(timezone.utc).isoformat(),
        }),
    }


def insert_transaction(cur, txn: dict):
    """Insère une nouvelle transaction dans la base PostgreSQL.
    
    Args:
        cur: Curseur de base de données PostgreSQL.
        txn (dict): Dictionnaire contenant les données de la transaction.
    """
    cur.execute("""
        INSERT INTO transactions (
            merchant_id, customer_id, pm_id, amount, currency, status,
            fraud_score, idempotency_key, device_type, ip_country, metadata
        ) VALUES (%(merchant_id)s, %(customer_id)s, %(pm_id)s, %(amount)s, %(currency)s,
                  %(status)s, %(fraud_score)s, %(idempotency_key)s, %(device_type)s,
                  %(ip_country)s, %(metadata)s)
    """, txn)


def main():
    """Point d'entrée principal du producer.
    
    Configure les arguments de ligne de commande (taux de transaction, ratio de fraude),
    établit la connexion à la base de données et boucle indéfiniment (jusqu'à interruption)
    pour générer et insérer des transactions simulées.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=float(os.environ.get("TXN_PRODUCER_RATE", 5)),
                        help="Transactions per second")
    parser.add_argument("--fraud-ratio", type=float, default=float(os.environ.get("FRAUD_RATIO", 0.05)),
                        help="Fraction of fraudulent transactions (0.0-1.0)")
    parser.add_argument("--max-txns", type=int, default=0,
                        help="Stop after N transactions (0 = infinite)")
    args = parser.parse_args()

    print(f"[START] Producer started: {args.rate} txn/s, {args.fraud_ratio*100:.1f}% fraud")
    if args.max_txns:
        print(f"   Will stop after {args.max_txns} transactions")

    sleep_per_txn = 1.0 / args.rate
    count = 0
    fraud_count = 0
    burst_remaining = 0
    burst_customer_id = None
    burst_pm_id = None
    burst_merchant_id = None

    with psycopg2.connect(**PG_CONFIG) as conn:
        with conn.cursor() as cur:
            while _running:
                loop_start = time.time()

                if burst_remaining > 0:
                    is_fraud = True
                    txn = build_transaction(is_fraud)
                    txn["merchant_id"] = burst_merchant_id
                    txn["customer_id"] = burst_customer_id
                    txn["pm_id"] = burst_pm_id
                    burst_remaining -= 1
                    if burst_remaining == 0:
                        print(f"  [BURST] completed for customer {burst_customer_id[:8]}…")
                else:
                    is_fraud = random.random() < args.fraud_ratio
                    txn = build_transaction(is_fraud)
                    txn["merchant_id"] = pick_merchant(cur)
                    customer_id, pm_id = pick_customer_pm(cur, is_fraud)
                    if not customer_id:
                        continue
                    txn["customer_id"] = customer_id
                    txn["pm_id"] = pm_id

                    # Burst = simulation de card testing / account takeover en rafale.
                    if is_fraud and random.random() < 0.3:
                        burst_remaining = random.randint(12, 20)
                        burst_customer_id = customer_id
                        burst_pm_id = pm_id
                        burst_merchant_id = txn["merchant_id"]
                        print(f"  [BURST] {burst_remaining} rapid txns for customer {customer_id[:8]}…")

                insert_transaction(cur, txn)
                conn.commit()

                count += 1
                if is_fraud:
                    fraud_count += 1

                if count % 25 == 0:
                    elapsed = time.time() - loop_start
                    print(f"  [{count:6d} txns, {fraud_count:4d} fraud] rate={1/max(elapsed, 0.001):.1f}/s")

                if args.max_txns and count >= args.max_txns:
                    print(f"[OK] Reached max_txns={args.max_txns}, stopping")
                    break

                # Pace — bursts go faster
                target_sleep = sleep_per_txn * 0.1 if burst_remaining > 0 else sleep_per_txn
                elapsed = time.time() - loop_start
                if elapsed < target_sleep:
                    time.sleep(target_sleep - elapsed)

    print(f"[OK] Producer stopped: {count} txns total, {fraud_count} fraud ({fraud_count/count*100 if count else 0:.1f}%)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Erreur producer: {e}", file=sys.stderr)
        sys.exit(1)
