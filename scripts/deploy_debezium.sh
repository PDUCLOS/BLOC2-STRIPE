#!/usr/bin/env bash
# Déploie le connecteur Debezium PostgreSQL → Kafka
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

set -a
source "$PROJECT_ROOT/.env"
set +a

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="stripe-postgres-cdc-v2"

echo "🔌 Déploiement du connecteur Debezium sur $CONNECT_URL..."

# Vérifie que Kafka Connect est up
for i in {1..30}; do
  if curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; then
    echo "✅ Kafka Connect ready"
    break
  fi
  echo "⏳ Waiting for Kafka Connect... ($i/30)"
  sleep 2
done

# Vérifie si le connecteur existe déjà
EXISTING=$(curl -fsS "$CONNECT_URL/connectors" 2>/dev/null | grep -o "$CONNECTOR_NAME" || true)
if [ -n "$EXISTING" ]; then
  echo "⚠️  Connecteur $CONNECTOR_NAME existe déjà, status :"
  curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool
  exit 0
fi

# Charge le template et substitue les variables
CONFIG=$(python3 - <<EOF
import json
from pathlib import Path
template = Path("$PROJECT_ROOT/config/debezium-connector.json").read_text()
config = json.loads(template)
config["config"]["database.hostname"] = "postgres"
config["config"]["database.user"] = "$PG_REPLICATION_USER"
config["config"]["database.password"] = "$PG_REPLICATION_PASSWORD"
print(json.dumps(config))
EOF
)

# POST le connecteur
HTTP_CODE=$(curl -s -o /tmp/debezium_response.txt -w "%{http_code}" \
  -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "$CONFIG")

if [ "$HTTP_CODE" = "201" ]; then
  echo "✅ Connecteur $CONNECTOR_NAME créé"
elif [ "$HTTP_CODE" = "409" ]; then
  echo "⚠️  Connecteur existe déjà (409 Conflict)"
else
  echo "❌ Échec création connecteur (HTTP $HTTP_CODE)"
  cat /tmp/debezium_response.txt
  exit 1
fi

echo ""
echo "📊 Status du connecteur :"
sleep 3
curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool
