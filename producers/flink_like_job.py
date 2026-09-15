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
sys.path.insert(0, str(_P(__file__).resolve().parent.parent))
import _env  # noqa: F401 — l'import seul déclenche le chargement du .env
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

# "rules" (défaut, sûr) ou "ml" pour utiliser le modèle XGBoost entraîné
# (ml/train_fraud_model.py). Si "ml" est demandé mais qu'aucun modèle n'a
# encore été entraîné, on retombe automatiquement sur "rules" (cf. score_transaction).
SCORING_ENGINE = os.environ.get("SCORING_ENGINE", "rules")

# Liste illustrative pour la démo (pays fréquemment cités dans les listes de risque
# carding/sanctions) — en prod ce serait piloté par un service de scoring pays
# tiers (ex. MaxMind, Sift) plutôt qu'une liste statique codée en dur.
HIGH_RISK_COUNTRIES = {"RU", "NG", "KP", "IR", "VE", "BY"}

_running = True


def signal_handler(sig, frame):
    """Coupe la boucle proprement sur Ctrl+C / SIGTERM, pas de kill -9 en plein scoring."""
    global _running
    print("\n[STOP] Stopping Flink-like job...")
    _running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def _backfill_velocity(pg_conn, r, customer_id, vkey_1h, vkey_24h, current_txn_id, now_ts):
    """Reconstruit v1h_<id>/v24h_<id> depuis Postgres au lieu de laisser Redis
    repartir de zéro pour ce client.

    Root cause corrigée ici (cf. docs/MLOPS.md §5.2) : le générateur cible
    délibérément des clients new/inactive pour ses patterns de fraude
    (producers/transaction_producer.py::pick_customer_pm()), donc le modèle
    apprend à raison "vélocité quasi nulle = fraude probable". Sans backfill,
    un simple FLUSHALL Redis (ou un redémarrage à froid du scorer) rend
    TOUS les clients — y compris les plus fidèles — indiscernables de
    vrais nouveaux clients, et déclenche une salve de faux positifs sur du
    trafic parfaitement légitime. On ne construit ce backfill qu'une seule
    fois par client et par process (appelé seulement si la clé Redis
    n'existe pas encore) — pas de coût Postgres sur le chemin chaud normal.
    """
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """SELECT EXTRACT(EPOCH FROM created_at) FROM transactions
                   WHERE customer_id = %s
                     AND created_at > NOW() - INTERVAL '24 hours'
                     AND txn_id != %s""",
                (customer_id, current_txn_id),
            )
            rows = cur.fetchall()
    except psycopg2.Error:
        pg_conn.rollback()
        return

    mapping_1h, mapping_24h = {}, {}
    for (ts,) in rows:
        ts = float(ts)
        mapping_24h[str(ts)] = ts
        if ts > now_ts - 3600:
            mapping_1h[str(ts)] = ts
    if mapping_1h:
        r.zadd(vkey_1h, mapping_1h)
    if mapping_24h:
        r.zadd(vkey_24h, mapping_24h)


def score_transaction(txn, r, pg_conn=None):
    """Calcule le score de fraude (logique identique au job PyFlink).

    Cette fonction s'appuie sur Redis pour stocker et récupérer la "vélocité"
    (nombre de transactions par heure/jour pour un client donné) afin
    d'ajuster le score de fraude en temps réel.

    Args:
        txn (dict): Dictionnaire de la transaction provenant de Kafka.
        r (redis.Redis): Instance de connexion à Redis.
        pg_conn (psycopg2.connection | None): utilisée pour reconstruire la
            vélocité depuis l'historique réel si la clé Redis n'existe pas
            encore (cf. _backfill_velocity) — None désactive le backfill
            (dégradé mais sûr : vélocité repart de 0 comme avant ce correctif).

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

        # Backfill AVANT le zadd de la transaction courante : si cette clé
        # n'a jamais existé pour ce process (premier passage pour ce client
        # depuis le dernier FLUSHALL/redémarrage), on restaure son vrai
        # historique plutôt que de laisser croire qu'il s'agit d'un nouveau
        # client — cf. _backfill_velocity ci-dessus.
        if pg_conn is not None and not r.exists(vkey_1h) and not r.exists(vkey_24h):
            _backfill_velocity(pg_conn, r, customer_id, vkey_1h, vkey_24h,
                                txn.get("txn_id"), now_ts)

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

    amount = txn.get("amount", 0) or 0
    country = str(txn.get("ip_country", "") or "")
    device = str(txn.get("device_type", "") or "")

    model_version = "rule-based-v1"
    score_val = None
    rules = []

    if SCORING_ENGINE == "ml":
        # Debezium (ExtractNewRecordState + TIMESTAMPTZ) sérialise created_at
        # en ISO 8601 — fallback sur l'heure courante si absent/mal formé
        # plutôt que de faire planter le scoring sur un champ non critique.
        try:
            raw_ts = txn.get("created_at")
            created_at = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00")) if raw_ts else datetime.now(timezone.utc)
        except (ValueError, TypeError):
            created_at = datetime.now(timezone.utc)

        from ml.scoring import score as ml_score
        ml_result = ml_score(
            amount=amount, created_at=created_at, ip_country=country,
            device_type=device, velocity_1h=velocity_1h, velocity_24h=velocity_24h,
        )
        if ml_result is not None:
            score_val = round(ml_result, 4)
            model_version = "xgboost-v1"
            # Le modèle ML ne produit pas de règles discrètes déclenchées —
            # rules_triggered reste vide pour ce model_version (le champ
            # existe toujours dans le schéma, juste non peuplé côté ML).
        # else : ml_result is None → modèle pas encore entraîné, on retombe
        # sur le moteur à règles ci-dessous (pas de branche `else` requise,
        # score_val reste None et le bloc suivant s'exécute).

    if score_val is None:
        # Moteur à règles (défaut, et fallback si SCORING_ENGINE=ml sans modèle entraîné).
        # Base à 0.1 pour représenter un risque résiduel minimal sur toute transaction.
        score_val = 0.1
        # Pondérations additives choisies pour que le cumul de 2-3 signaux faibles
        # franchisse REVIEW_THRESHOLD (0.6), et qu'un signal fort isolé (géo à risque,
        # montant élevé) s'en approche déjà seul — évite qu'une seule règle domine
        # totalement la décision.
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
        model_version = "rule-based-v1"

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
    txn["model_version"] = model_version
    txn["scored_at"] = datetime.now(timezone.utc).isoformat()

    return txn


def main():
    """Boucle de scoring : lit le CDC Postgres, score avec Redis en feature store, sink Kafka, et
    write-back du fraud_score en base pour que le dashboard SQL soit à jour direct (pas de JOIN Kafka)."""
    print(f"[START] Flink-like job started")
    print(f"   Kafka brokers: {KAFKA_BROKERS}")
    print(f"   Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"   Scoring engine: {SCORING_ENGINE}"
          + (" (fallback auto sur rules si modèle absent)" if SCORING_ENGINE == "ml" else ""))

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
        print(f"[OK] Redis connected")
    except redis.RedisError as e:
        print(f"[ERROR] Redis connection failed: {e}")
        sys.exit(1)

    # Consumer Kafka
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "flink-fraud-scorer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(["stripe.public.transactions"])
    print(f"[OK] Kafka consumer subscribed to stripe.public.transactions")

    # Producer Kafka (pour les sinks)
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKERS,
        "linger.ms": 50,
        "compression.type": "none",
    })

    # Connexion Postgres (pour write-back du fraud_score)
    # Note: Debezium capte cet UPDATE et le republie sur stripe.public.transactions
    # — le garde-fou `if txn.get("fraud_score") is not None: continue` plus haut
    # dans la boucle est ce qui empêche ce cycle de re-scorer la transaction.
    def connect_pg():
        """Ouvre une connexion Postgres pour le write-back, ou None si indisponible.

        connect_timeout court : en cas de Postgres down, on ne veut pas
        bloquer la boucle de scoring en attendant une connexion qui échouera.
        """
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASSWORD,
                connect_timeout=3,
            )
            print(f"[OK] PostgreSQL connected (for fraud_score write-back)")
            return conn
        except psycopg2.OperationalError as e:
            print(f"[WARN] PostgreSQL not reachable ({e}), fraud_score write-back disabled")
            return None

    pg_conn = connect_pg()
    # Reconnexion throttlée : au minimum PG_RECONNECT_INTERVAL_SECONDS entre
    # deux tentatives, pour ne pas retenter une connexion à chaque transaction
    # (des centaines/s) tant que Postgres reste down.
    PG_RECONNECT_INTERVAL_SECONDS = 10
    last_pg_reconnect_attempt = time.time()

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

        # Ignore l'écho de notre propre write-back : l'UPDATE fraud_score
        # plus bas est capté par Debezium et republié sur ce MÊME topic
        # (stripe.public.transactions), donc CHAQUE transaction produit 2
        # événements CDC — l'INSERT original (fraud_score=null) et cet
        # UPDATE (fraud_score déjà renseigné). Sans ce garde-fou, le 2e
        # passage réincrémente la vélocité Redis et rescore/ré-émet vers
        # Mongo pour une transaction déjà traitée — vélocité et alertes
        # doublées en continu, ce qui désynchronise les features servies de
        # la distribution vue à l'entraînement (train/serve skew) et a
        # fait chuter la précision réelle à ~29% en prod (cf. investigation
        # du 2026-09-15, ml_monitoring status=alert répété malgré retrain).
        if txn.get("fraud_score") is not None:
            continue

        # Scoring : si KO (txn malformé, pas de customer_id, etc), DLQ aussi
        try:
            scored = score_transaction(txn, r, pg_conn)
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

        # Reconnexion si la connexion précédente est morte (throttlée pour ne
        # pas retenter à chaque message tant que Postgres est down).
        if pg_conn is None and time.time() - last_pg_reconnect_attempt >= PG_RECONNECT_INTERVAL_SECONDS:
            last_pg_reconnect_attempt = time.time()
            pg_conn = connect_pg()

        # Write-back fraud_score dans Postgres (optionnel, mais ferme la boucle)
        # et aligne le dashboard OLTP avec la décision temps réel.
        #
        # Deux écritures dans UNE seule transaction Postgres (atomicité ACID) :
        #   1. UPDATE transactions.fraud_score — source : score calculé ci-dessus
        #      (score_transaction), clé : txn_id reçu du CDC Debezium.
        #   2. INSERT fraud_indicators — uniquement pour les décisions review/block,
        #      et uniquement si l'UPDATE a réellement modifié la ligne (rowcount=1).
        # Le filtre `fraud_score IS NULL` rend l'ensemble idempotent : un message
        # CDC rejoué (redémarrage consumer, rééquilibrage Kafka) ou l'événement
        # CDC généré par notre propre UPDATE ne produit ni second score ni
        # indicateur en double. Si l'INSERT échoue, le rollback annule aussi
        # l'UPDATE : jamais de score "block" sans sa trace dans fraud_indicators.
        if pg_conn is not None and scored.get("fraud_score") is not None:
            try:
                with pg_conn.cursor() as cur:
                    cur.execute(
                        """UPDATE transactions
                           SET fraud_score = %s
                           WHERE txn_id = %s AND fraud_score IS NULL""",
                        (scored["fraud_score"], scored["txn_id"]),
                    )
                    if cur.rowcount == 1 and scored.get("decision") in ("review", "block"):
                        cur.execute(
                            """INSERT INTO fraud_indicators
                                   (txn_id, anomaly_score, rules_triggered, model_version, decision)
                               VALUES (%s, %s, %s, %s, %s)""",
                            (
                                scored["txn_id"],
                                scored["fraud_score"],
                                # TEXT[] Postgres ← list Python (psycopg2 adapte nativement).
                                # Vide pour xgboost-v1 : le modèle ne produit pas de règles.
                                scored.get("rules_triggered") or [],
                                scored.get("model_version"),
                                scored["decision"],
                            ),
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

    print(f"[OK] Stopped: {count} txns, {alert_count} alerts, {writeback_count} writebacks")
    producer.flush(5)
    consumer.close()
    if pg_conn is not None:
        pg_conn.close()


if __name__ == "__main__":
    main()
