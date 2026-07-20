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

FRAUD_THRESHOLD  = float(os.environ.get("FRAUD_THRESHOLD", 0.85))
REVIEW_THRESHOLD = float(os.environ.get("REVIEW_THRESHOLD", 0.60))

HIGH_RISK_COUNTRIES = {"RU", "NG", "KP", "IR", "VE", "BY"}

SOURCE_TOPIC  = "stripe.public.transactions"
SINK_TOPIC    = "stripe.payments.events"
ALERTS_TOPIC  = "stripe.fraud.alerts"
DLQ_TOPIC     = "stripe.etl.dead-letter"


# ─── Scoring ───────────────────────────────────────────────────────────────────
class FraudScoringFunction(MapFunction):
    """Calcule le fraud_score pour chaque événement CDC Debezium."""

    def __init__(self):
        self.redis_client = None

    def open(self, runtime_context):
        import redis
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )

    def map(self, raw):
        try:
            event = json.loads(raw)
            after = event.get("payload", {}).get("after") or event.get("after", {})
            if not after:
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
            self.redis_client.zadd(v_key_1h,  {str(now_ts): now_ts})
            self.redis_client.zadd(v_key_24h, {str(now_ts): now_ts})
            self.redis_client.expire(v_key_1h, 3700)
            self.redis_client.expire(v_key_24h, 87000)
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
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host=PG_HOST, dbname=PG_DB,
                    user=PG_USER, password=PG_PASSWORD
                )
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE transactions SET fraud_score = %s WHERE txn_id = %s::uuid",
                        (score, txn_id)
                    )
                conn.commit()
                conn.close()
            except Exception:
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
