#!/usr/bin/env python3
"""
PyFlink Fraud Scoring Job — Production.
Déployer via : make flink-submit

Équivalent de producers/flink_like_job.py mais sur cluster Flink natif.
Lit stripe.public.transactions, score chaque événement CDC Debezium,
écrit stripe.payments.events + stripe.fraud.alerts.
"""
import os
import json
import time
from datetime import datetime, timezone

# PyFlink imports — disponibles uniquement dans le conteneur Flink
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction, FilterFunction

# ─── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKERS    = os.environ.get("KAFKA_BROKERS", "kafka:9092")
REDIS_HOST       = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT       = int(os.environ.get("REDIS_PORT", 6379))
PG_HOST          = os.environ.get("PG_HOST", "postgres")
PG_DB            = os.environ.get("PG_DB", "stripe_oltp")
PG_USER          = os.environ.get("PG_USER", "stripe_app")
PG_PASSWORD      = os.environ.get("PG_PASSWORD", "")

# Mêmes noms que producers/flink_like_job.py et .env.example (anciens noms en repli).
FRAUD_THRESHOLD  = float(os.environ.get("FRAUD_SCORE_THRESHOLD", os.environ.get("FRAUD_THRESHOLD", 0.85)))
REVIEW_THRESHOLD = float(os.environ.get("REVIEW_SCORE_THRESHOLD", os.environ.get("REVIEW_THRESHOLD", 0.60)))

HIGH_RISK_COUNTRIES = {"RU", "NG", "KP", "IR", "VE", "BY"}

SOURCE_TOPIC  = "stripe.public.transactions"
SINK_TOPIC    = "stripe.payments.events"
ALERTS_TOPIC  = "stripe.fraud.alerts"
DLQ_TOPIC     = "stripe.etl.dead-letter"


def _backfill_velocity(pg_conn, redis_client, customer_id, v_key_1h, v_key_24h, current_txn_id, now_ts):
    """Reconstruit v1h_<id>/v24h_<id> depuis Postgres au lieu de laisser Redis
    repartir de zéro pour ce client — même correctif que
    producers/flink_like_job.py::_backfill_velocity (cf. docs/MLOPS.md §5.2) :
    sans ça, un FLUSHALL Redis rend tout client indiscernable d'un vrai
    nouveau client, et le modèle ML (qui a appris "vélocité quasi nulle =
    fraude probable" — le générateur cible les clients new/inactive pour ses
    patterns de fraude) déclenche une salve de faux positifs sur du trafic
    légitime.
    """
    import psycopg2
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
        redis_client.zadd(v_key_1h, mapping_1h)
    if mapping_24h:
        redis_client.zadd(v_key_24h, mapping_24h)


# ─── Scoring ───────────────────────────────────────────────────────────────────
class FraudScoringFunction(MapFunction):
    """Calcule le fraud_score pour chaque événement CDC Debezium."""

    def __init__(self):
        self.redis_client = None
        self.pg_conn = None

    def open(self, runtime_context):
        # open() s'exécute une fois par instance de tâche sur le TaskManager
        # (pas par événement) : c'est là, et pas dans __init__, qu'on peut
        # créer les connexions Redis/Postgres car __init__ tourne côté client
        # avant sérialisation de la fonction vers les workers Flink.
        import redis
        import psycopg2
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        try:
            self.pg_conn = psycopg2.connect(
                host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
                connect_timeout=3,
            )
        except psycopg2.OperationalError:
            self.pg_conn = None  # backfill désactivé, dégradé mais sûr

    def map(self, raw):
        try:
            event = json.loads(raw)
            # Debezium envoie l'état courant dans payload.after pour les events utiles.
            after = event.get("payload", {}).get("after") or event.get("after", {})
            if not after:
                return json.dumps({"_skip": True, "raw": raw})

            # Ignore l'écho de notre propre write-back : l'UPDATE fraud_score
            # plus bas est capté par Debezium et republié sur ce MÊME topic,
            # donc sans ce garde-fou chaque transaction serait rescorée
            # (et sa vélocité Redis réincrémentée) indéfiniment — même bug
            # que producers/flink_like_job.py, corrigé ici en miroir.
            if after.get("fraud_score") is not None:
                return json.dumps({"_skip": True, "raw": raw})

            txn_id      = str(after.get("txn_id", ""))
            customer_id = str(after.get("customer_id", ""))
            amount      = int(after.get("amount", 0))
            ip_country  = after.get("ip_country", "")
            device_type = after.get("device_type", "")
            now_ts      = time.time()

            # Vélocité Redis
            v_key_1h  = f"v1h_{customer_id}"
            v_key_24h = f"v24h_{customer_id}"
            # Backfill AVANT le zadd de la transaction courante : cf.
            # _backfill_velocity ci-dessus.
            if (self.pg_conn is not None
                    and not self.redis_client.exists(v_key_1h)
                    and not self.redis_client.exists(v_key_24h)):
                _backfill_velocity(self.pg_conn, self.redis_client, customer_id,
                                    v_key_1h, v_key_24h, txn_id, now_ts)
            self.redis_client.zadd(v_key_1h,  {str(now_ts): now_ts})
            self.redis_client.zadd(v_key_24h, {str(now_ts): now_ts})
            self.redis_client.expire(v_key_1h, 3700)
            # 86500 = 86400 (24h) + marge de 100s, alignée sur producers/flink_like_job.py
            # (les deux implémentations doivent expirer selon la même règle).
            self.redis_client.expire(v_key_24h, 86500)
            velocity_1h  = self.redis_client.zcount(v_key_1h,  now_ts - 3600,  "+inf")
            velocity_24h = self.redis_client.zcount(v_key_24h, now_ts - 86400, "+inf")

            # Règles de scoring
            score = 0.0
            rules = []

            if amount > 100_000:                           # R1 : montant > 1000€
                score += 0.35
                rules.append("R1_high_amount")

            if 0 < amount < 200 and device_type in ("mobile", "pos"):  # R2 : card testing
                score += 0.15
                rules.append("R2_card_testing")

            if ip_country in HIGH_RISK_COUNTRIES:          # R3 : pays risqué
                score += 0.40
                rules.append("R3_high_risk_geo")

            if velocity_1h > 10:                           # R4 : vélocité > 10 txns/h
                score += 0.25
                rules.append("R4_velocity_1h")

            if velocity_24h > 50:                          # R5 : vélocité > 50 txns/24h
                score += 0.15
                rules.append("R5_velocity_24h")

            score = min(round(score, 3), 1.0)

            if score >= FRAUD_THRESHOLD:
                decision = "block"
            elif score >= REVIEW_THRESHOLD:
                decision = "review"
            else:
                decision = "allow"

            # Mise à jour PostgreSQL (write-back)
            # Connexion ouverte/fermée à chaque événement plutôt que réutilisée :
            # simplicité de référence pour la démo (pas de pool psycopg2 partagé
            # entre workers Flink). À remplacer par un pool en prod à fort débit.
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host=PG_HOST, dbname=PG_DB,
                    user=PG_USER, password=PG_PASSWORD
                )
                # Même contrat que producers/flink_like_job.py : UPDATE idempotent
                # (fraud_score IS NULL) + INSERT fraud_indicators pour review/block,
                # dans une seule transaction Postgres.
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE transactions SET fraud_score = %s "
                        "WHERE txn_id = %s::uuid AND fraud_score IS NULL",
                        (score, txn_id)
                    )
                    if cur.rowcount == 1 and decision in ("review", "block"):
                        cur.execute(
                            "INSERT INTO fraud_indicators "
                            "(txn_id, anomaly_score, rules_triggered, model_version, decision) "
                            "VALUES (%s::uuid, %s, %s, %s, %s)",
                            (txn_id, score, rules, "rule-based-v1", decision)
                        )
                conn.commit()
                conn.close()
            except Exception:
                # Le scoring continue même si le write-back échoue ponctuellement.
                pass  # Non bloquant

            result = {
                **after,
                "fraud_score":  score,
                "decision":     decision,
                "rules_fired":  rules,
                "scored_at":    datetime.now(timezone.utc).isoformat(),
            }
            return json.dumps(result)

        except Exception as e:
            return json.dumps({"_dlq": True, "error": str(e), "raw": raw})


class FraudAlertFilter(FilterFunction):
    def filter(self, value):
        try:
            d = json.loads(value)
            return d.get("decision") in ("block", "review") and not d.get("_skip")
        except Exception:
            return False


# ─── Main ───────────────────────────────────────────────────────────────────────
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Parallelism ajusté pour la démo locale; à relever en cluster selon partitions Kafka.
    env.set_parallelism(2)

    # Source Kafka
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-fraud-scorer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "Kafka-CDC-Debezium"
    )

    # Scoring
    scored = stream.map(FraudScoringFunction())

    # Sink principal
    scored.sink_to(
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(SINK_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # Sink alertes fraude
    scored.filter(FraudAlertFilter()).sink_to(
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(ALERTS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    env.execute("Stripe-FraudScoringJob")


if __name__ == "__main__":
    main()
