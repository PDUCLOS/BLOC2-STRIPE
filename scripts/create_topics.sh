#!/usr/bin/env bash
# Crée les topics Kafka applicatifs
# Les topics CDC (stripe.public.*) sont créés automatiquement par Debezium
# lors du déploiement du connector
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

set -a
source "$PROJECT_ROOT/.env"
set +a

KAFKA_BOOTSTRAP="${KAFKA_BROKERS:-localhost:9092}"

echo "📨 Création des topics Kafka sur $KAFKA_BOOTSTRAP..."

create_topic() {
  local topic=$1
  local partitions=$2
  local retention_ms=$3
  docker exec stripe-kafka kafka-topics \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "retention.ms=$retention_ms" \
    --config "cleanup.policy=delete" 2>&1 | grep -E "^(Created|Error|WARNING)" || true
}

# Topics applicatifs
create_topic "stripe.payments.events"      12  2592000000  # 30 jours
create_topic "stripe.fraud.alerts"         3  2592000000  # 30 jours
create_topic "stripe.etl.dead-letter"      3  -1          # rétention infinie

echo ""
echo "📋 Topics existants :"
docker exec stripe-kafka kafka-topics \
  --bootstrap-server "$KAFKA_BOOTSTRAP" --list | grep -E "^stripe\." || echo "(aucun pour l'instant — Debezium les créera au déploiement du connector)"

echo ""
echo "✅ Topics applicatifs créés"
