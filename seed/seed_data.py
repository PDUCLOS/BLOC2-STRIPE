#!/usr/bin/env python3
"""Seed initial data: merchants, customers, payment_methods.

Inserts realistic-ish data into the OLTP PostgreSQL DB so the producer
has something to work with. Idempotent: safe to re-run (TRUNCATE first).
"""
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

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

# Volume par défaut
N_MERCHANTS = int(os.environ.get("SEED_MERCHANTS", 200))
N_CUSTOMERS = int(os.environ.get("SEED_CUSTOMERS", 5000))
N_PM_PER_CUSTOMER = 2  # average

# Pays et tiers réalistes
COUNTRIES = ["FR", "US", "GB", "DE", "ES", "IT", "NL", "BE", "CA", "JP", "AU", "BR", "MX", "IN"]
SEGMENTS = ["premium", "standard", "new", "inactive"]
TIERS = ["standard", "premium", "enterprise"]
PAYMENT_METHOD_TYPES = ["card", "wallet", "bank_transfer"]
CARD_BRANDS = ["visa", "mastercard", "amex", "cb"]

fake = Faker()
Faker.seed(42)
random.seed(42)


def insert_merchants(cur):
    """Génère et insère un jeu de données de marchands simulés dans PostgreSQL.
    
    Args:
        cur: Curseur de la base de données PostgreSQL.
    """
    print(f"  → Inserting {N_MERCHANTS} merchants...")
    rows = []
    used_emails = set()
    for _ in range(N_MERCHANTS):
        name = fake.company()
        # Make email unique
        while True:
            email = f"{fake.user_name()}_{random.randint(1000, 9999)}@{fake.domain_name()}"
            if email not in used_emails:
                used_emails.add(email)
                break
        rows.append((
            name,
            email,
            random.choice(COUNTRIES),
            random.choices(TIERS, weights=[60, 30, 10])[0],
            "active",
        ))
    execute_values(
        cur,
        "INSERT INTO merchants (name, email, country_code, tier, status) VALUES %s "
        "ON CONFLICT (email) DO NOTHING",
        rows,
    )


def insert_customers(cur):
    """Génère et insère un jeu de données de clients simulés dans PostgreSQL.
    
    Args:
        cur: Curseur de la base de données PostgreSQL.
    """
    print(f"  → Inserting {N_CUSTOMERS} customers...")
    rows = []
    used_emails = set()
    for _ in range(N_CUSTOMERS):
        while True:
            email = f"{fake.user_name()}_{random.randint(10000, 99999)}@example.com"
            if email not in used_emails:
                used_emails.add(email)
                break
        rows.append((
            email,
            fake.name(),
            random.choice(COUNTRIES),
            random.choices(SEGMENTS, weights=[15, 50, 25, 10])[0],
        ))
    execute_values(
        cur,
        "INSERT INTO customers (email, name, country_code, segment) VALUES %s "
        "ON CONFLICT DO NOTHING",
        rows,
    )


def insert_payment_methods(cur):
    """Génère et insère des méthodes de paiement pour les clients existants.
    
    Attribue un nombre aléatoire de méthodes de paiement par client, avec
    une préférence pour les cartes bancaires, et définit éventuellement
    une méthode par défaut.
    
    Args:
        cur: Curseur de la base de données PostgreSQL.
    """
    cur.execute("SELECT customer_id FROM customers")
    customers = [r[0] for r in cur.fetchall()]
    print(f"  → Inserting payment methods for {len(customers)} customers...")

    rows = []
    for cid in customers:
        n = max(1, int(random.gauss(N_PM_PER_CUSTOMER, 0.7)))
        has_default = False
        for _ in range(n):
            pm_type = random.choices(PAYMENT_METHOD_TYPES, weights=[85, 12, 3])[0]
            brand = random.choice(CARD_BRANDS) if pm_type == "card" else None
            fingerprint = fake.sha256() if pm_type == "card" else None
            is_default = (not has_default) and (random.random() < 0.4)
            if is_default:
                has_default = True
            rows.append((
                cid,
                pm_type,
                brand,
                fingerprint,
                is_default,
                (datetime.now(timezone.utc) + timedelta(days=random.randint(180, 1825))).date(),
            ))

    execute_values(
        cur,
        "INSERT INTO payment_methods (customer_id, type, brand, fingerprint, is_default, expires_at) VALUES %s",
        rows,
        page_size=1000,
    )


def main():
    """Point d'entrée principal pour l'initialisation des données (seed).
    
    1. Se connecte à PostgreSQL.
    2. Vide les tables existantes (TRUNCATE CASCADE).
    3. Insère les marchands, les clients et les méthodes de paiement.
    4. Affiche un résumé du nombre d'enregistrements créés.
    """
    print("🌱 Seeding data...")
    with psycopg2.connect(**PG_CONFIG) as conn:
        with conn.cursor() as cur:
            # Reset (cascade to PM)
            print("  → Truncating existing data...")
            cur.execute("TRUNCATE merchants, customers, payment_methods, transactions, refunds, fraud_indicators RESTART IDENTITY CASCADE")

            insert_merchants(cur)
            insert_customers(cur)
            insert_payment_methods(cur)

            # Count check
            cur.execute("SELECT count(*) FROM merchants")
            print(f"  ✓ Merchants: {cur.fetchone()[0]}")
            cur.execute("SELECT count(*) FROM customers")
            print(f"  ✓ Customers: {cur.fetchone()[0]}")
            cur.execute("SELECT count(*) FROM payment_methods")
            print(f"  ✓ Payment methods: {cur.fetchone()[0]}")

        conn.commit()
    print("✅ Seed terminé")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Erreur seed: {e}", file=sys.stderr)
        sys.exit(1)
