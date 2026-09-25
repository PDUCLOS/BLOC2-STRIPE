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
# Ajoute la racine du projet au path Python pour que le helper _env soit importable
# quand le script est lancé directement depuis le dossier producers/.
sys.path.insert(0, str(_P(__file__).resolve().parent.parent))
import _env  # noqa: F401
# Paramètres de connexion PostgreSQL lus depuis l'environnement préparé par _env.
PG_CONFIG = {
    "host": os.environ["PG_HOST"],
    "port": int(os.environ.get("PG_PORT", 5432)),
    "dbname": os.environ["PG_DB"],
    "user": os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
}

# Référentiels utilisés pour fabriquer des transactions crédibles et variées.
DEVICE_TYPES = ["mobile", "desktop", "tablet", "pos"]
CURRENCIES = ["EUR", "USD", "GBP", "JPY", "CAD", "AUD"]
FRAUD_COUNTRIES = ["RU", "NG", "KP", "IR", "VE"]  # high-risk geo
NORMAL_COUNTRIES = ["FR", "US", "GB", "DE", "ES", "IT", "NL", "BE", "CA"]

# Flag global contrôlé par les signaux système pour arrêter proprement la boucle.
_running = True


# -----------------------------------------------------------------------------
# Gestion des signaux et sélection des données
# -----------------------------------------------------------------------------

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
    # On répartit les transactions sur les marchands actifs pour simuler un flux réel.
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
        # On favorise les profils plus risqués pour augmenter la proportion de cas détectables.
        cur.execute("""
            SELECT c.customer_id, pm.pm_id
            FROM customers c
            JOIN payment_methods pm ON pm.customer_id = c.customer_id
            WHERE c.segment IN ('new', 'inactive')
            ORDER BY random() LIMIT 1
        """)
    else:
        # Cas normal : clients établis avec comportement plus standard.
        cur.execute("""
            SELECT c.customer_id, pm.pm_id
            FROM customers c
            JOIN payment_methods pm ON pm.customer_id = c.customer_id
            WHERE c.segment IN ('standard', 'premium')
            ORDER BY random() LIMIT 1
        """)
    row = cur.fetchone()
    return row if row else (None, None)


# -----------------------------------------------------------------------------
# Construction du payload transaction
# -----------------------------------------------------------------------------

# Profils de fraude : la proportion de fraudes « furtives » plafonne volontairement
# le rappel atteignable, et le bruit côté légitime plafonne la précision. Sans
# cela, les features (montant, pays, appareil, vélocité) recodaient exactement la
# recette du générateur et le modèle affichait 0,99 de précision servie — un
# chiffre qui mesurait la fidélité du pipeline, pas la difficulté du problème.
FRAUD_PROFILES = [("brutal", 0.35), ("card_testing", 0.25), ("furtive", 0.40)]


def pick_fraud_profile() -> str:
    """Tire un profil de fraude selon FRAUD_PROFILES."""
    r, acc = random.random(), 0.0
    for name, weight in FRAUD_PROFILES:
        acc += weight
        if r < acc:
            return name
    return FRAUD_PROFILES[-1][0]


def build_transaction(is_fraud: bool, profile: str = None) -> dict:
    """Construit le payload d'une transaction.

    Fraude (is_fraud=True), selon le profil :
      - brutal       : gros montant (100 → 2 000 €), 60 % pays à risque, 30 % appareil POS
      - card_testing : petits montants (1 → 5 €) répétés en rafale, pays normal
      - furtive      : montant, pays et appareil ordinaires — indiscernable par les
                       features actuelles (compte récent ou inactif seulement)
    Légitime : bruit réaliste — voyageurs depuis un pays à risque (3 %), terminal
    POS (8 %), gros achats (2 %) — pour que les signaux « fraude » ne soient pas
    des preuves.
    """
    profile = profile or (pick_fraud_profile() if is_fraud else None)

    amount = random.randint(50, 50000)  # centimes, 0.50 € → 500 €
    ip_country = random.choice(NORMAL_COUNTRIES)
    device = random.choice(DEVICE_TYPES[:3])  # mobile, desktop, tablet

    if profile == "brutal":
        amount = random.randint(10000, 200000)  # 100 € → 2 000 €
        if random.random() < 0.6:
            ip_country = random.choice(FRAUD_COUNTRIES)
        if random.random() < 0.3:
            device = "pos"
    elif profile == "card_testing":
        amount = random.randint(100, 500)  # 1 € → 5 €
        if random.random() < 0.2:
            ip_country = random.choice(FRAUD_COUNTRIES)
    elif profile == "furtive":
        amount = random.randint(2000, 60000)  # 20 € → 600 € : dans la masse
    else:
        # Légitime : quelques comportements atypiques mais honnêtes
        if random.random() < 0.03:
            ip_country = random.choice(FRAUD_COUNTRIES)  # voyage, expatriation
        if random.random() < 0.08:
            device = "pos"  # paiement en boutique
        if random.random() < 0.02:
            amount = random.randint(50000, 200000)  # achat premium 500 → 2 000 €
    currency = random.choice(CURRENCIES)

    # user_agent : indice, pas preuve — la moitié des fraudes utilisent un navigateur normal.
    browser_ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
    user_agent = browser_ua if (not is_fraud or random.random() < 0.5) else "curl/8.4.0"

    # Le payload reste volontairement proche du schéma attendu en base.
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
            "user_agent": user_agent,
            "session_id": str(uuid.uuid4()),
            "is_fraud_pattern": is_fraud,
            "fraud_profile": profile,
            "producer_ts": datetime.now(timezone.utc).isoformat(),
        }),
    }


def insert_transaction(cur, txn: dict):
    """Insère une nouvelle transaction dans la base PostgreSQL.
    
    Args:
        cur: Curseur de base de données PostgreSQL.
        txn (dict): Dictionnaire contenant les données de la transaction.
    """
    # L'INSERT est paramétré pour éviter toute concaténation de chaînes SQL.
    cur.execute("""
        INSERT INTO transactions (
            merchant_id, customer_id, pm_id, amount, currency, status,
            fraud_score, idempotency_key, device_type, ip_country, metadata
        ) VALUES (%(merchant_id)s, %(customer_id)s, %(pm_id)s, %(amount)s, %(currency)s,
                  %(status)s, %(fraud_score)s, %(idempotency_key)s, %(device_type)s,
                  %(ip_country)s, %(metadata)s)
    """, txn)


# -----------------------------------------------------------------------------
# Boucle principale de génération
# -----------------------------------------------------------------------------

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

    # Conversion du débit cible en temps d'attente entre deux insertions.
    sleep_per_txn = 1.0 / args.rate
    count = 0
    fraud_count = 0
    # Variables de contexte pour simuler une rafale de fraude sur un même compte.
    burst_remaining = 0
    burst_is_fraud = False
    burst_profile = None
    burst_customer_id = None
    burst_pm_id = None
    burst_merchant_id = None

    # Une seule connexion est gardée ouverte pour éviter le coût de reconnexion.
    with psycopg2.connect(**PG_CONFIG) as conn:
        with conn.cursor() as cur:
            while _running:
                loop_start = time.time()

                # Un "burst" en cours prend la main sur la génération normale : on rejoue
                # le même triplet marchand/client/moyen de paiement pour simuler une rafale
                # d'essais de carte volée sur un seul compte (au lieu d'une fraude isolée).
                if burst_remaining > 0:
                    is_fraud = burst_is_fraud
                    txn = build_transaction(is_fraud, burst_profile)
                    txn["merchant_id"] = burst_merchant_id
                    txn["customer_id"] = burst_customer_id
                    txn["pm_id"] = burst_pm_id
                    burst_remaining -= 1
                    if burst_remaining == 0:
                        print(f"  [BURST] completed for customer {burst_customer_id[:8]}…")
                else:
                    is_fraud = random.random() < args.fraud_ratio
                    txn = build_transaction(is_fraud)
                    profile = json.loads(txn["metadata"]).get("fraud_profile")
                    txn["merchant_id"] = pick_merchant(cur)
                    customer_id, pm_id = pick_customer_pm(cur, is_fraud)
                    if not customer_id:
                        # Si aucun profil adéquat n'est trouvé, on saute cette itération.
                        continue
                    txn["customer_id"] = customer_id
                    txn["pm_id"] = pm_id

                    # Rafales : card testing (toujours), fraude brutale (1 fois sur 3) —
                    # jamais la fraude furtive. Côté légitime, 4 % des clients enchaînent
                    # aussi plusieurs achats (billets, panier en plusieurs fois) : la
                    # vélocité seule ne prouve donc rien.
                    burst_len = 0
                    if profile == "card_testing":
                        burst_len = random.randint(4, 10)
                    elif profile == "brutal" and random.random() < 0.3:
                        burst_len = random.randint(3, 8)
                    elif not is_fraud and random.random() < 0.04:
                        burst_len = random.randint(3, 6)
                    if burst_len:
                        burst_remaining = burst_len
                        burst_is_fraud = is_fraud
                        burst_profile = profile
                        burst_customer_id = customer_id
                        burst_pm_id = pm_id
                        burst_merchant_id = txn["merchant_id"]
                        print(f"  [BURST] {burst_remaining} rapid txns ({profile or 'legit'}) for customer {customer_id[:8]}…")

                insert_transaction(cur, txn)
                conn.commit()

                # On suit le volume total et le volume frauduleux pour l'affichage console.
                count += 1
                if is_fraud:
                    fraud_count += 1

                if count % 25 == 0:
                    elapsed = time.time() - loop_start
                    print(f"  [{count:6d} txns, {fraud_count:4d} fraud] rate={1/max(elapsed, 0.001):.1f}/s")

                if args.max_txns and count >= args.max_txns:
                    print(f"[OK] Reached max_txns={args.max_txns}, stopping")
                    break

                # Rafales plus rapides que le flux normal ; une rafale légitime est plus lente
                # qu'un card testing (0,3 contre 0,1 du pas nominal).
                if burst_remaining > 0:
                    target_sleep = sleep_per_txn * (0.1 if burst_is_fraud else 0.3)
                else:
                    target_sleep = sleep_per_txn
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
