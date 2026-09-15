#!/usr/bin/env bash
# Déploie le connecteur Debezium PostgreSQL → Kafka
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

set -a
source "$PROJECT_ROOT/.env"
set +a

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
# Suffixe -v2 : change le nom si la config du connecteur évolue de façon
# incompatible (ex. slot.name, publication), pour éviter de réutiliser un
# connecteur existant avec un replication slot Postgres orphelin de l'ancienne config.
CONNECTOR_NAME="stripe-postgres-cdc-v2"

echo "Déploiement du connecteur Debezium sur $CONNECT_URL..."

# Vérifie que Kafka Connect est up
for i in {1..30}; do
  if curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; then
    echo "[OK] Kafka Connect ready"
    break
  fi
  echo "[WAIT] Waiting for Kafka Connect... ($i/30)"
  sleep 2
done

# Vérifie si le connecteur existe déjà
EXISTING=$(curl -fsS "$CONNECT_URL/connectors" 2>/dev/null | grep -o "$CONNECTOR_NAME" || true)
if [ -n "$EXISTING" ]; then
  echo "[WARN] Connecteur $CONNECTOR_NAME existe déjà, status :"
  # Juste après la création, Kafka Connect peut encore répondre 404 le temps
  # d'enregistrer le connecteur (rebalance du groupe) : on réessaie jusqu'à 30 s.
  for _ in $(seq 1 15); do
    if STATUS=$(curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null); then break; fi
    STATUS=""; sleep 2
  done
  [ -n "$STATUS" ] || { echo "[ERROR] statut du connecteur indisponible après 30 s"; exit 1; }
  echo "$STATUS" | python3 -m json.tool
  exit 0
fi

# Charge le template et substitue les variables
# Le template versionné évite de disperser la config CDC dans les scripts shell.
CONFIG=$(python3 - <<EOF
import json
from pathlib import Path
template = Path("$PROJECT_ROOT/config/debezium-connector.json").read_text()
config = json.loads(template)
# Le nom vient du script (CONNECTOR_NAME) et non du template : sinon le connecteur
# est créé sous un autre nom et la lecture de son statut renvoie 404.
config["name"] = "$CONNECTOR_NAME"
config["config"]["database.hostname"] = "postgres"
config["config"]["database.user"] = "$PG_REPLICATION_USER"
config["config"]["database.password"] = "$PG_REPLICATION_PASSWORD"
print(json.dumps(config))
EOF
)

# POST le connecteur
# Le code HTTP est inspecté explicitement pour distinguer création, conflit et erreur.
HTTP_CODE=$(curl -s -o /tmp/debezium_response.txt -w "%{http_code}" \
  -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "$CONFIG")

if [ "$HTTP_CODE" = "201" ]; then
  echo "[OK] Connecteur $CONNECTOR_NAME créé"
elif [ "$HTTP_CODE" = "409" ]; then
  echo "[WARN] Connecteur existe déjà (409 Conflict)"
else
  echo "[ERROR] Échec création connecteur (HTTP $HTTP_CODE)"
  cat /tmp/debezium_response.txt
  exit 1
fi

echo ""
echo "Status du connecteur :"
sleep 3
# Juste après la création, Kafka Connect peut encore répondre 404 le temps
# d'enregistrer le connecteur (rebalance du groupe) : on réessaie jusqu'à 30 s.
for _ in $(seq 1 15); do
  if STATUS=$(curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null); then break; fi
  STATUS=""; sleep 2
done
[ -n "$STATUS" ] || { echo "[ERROR] statut du connecteur indisponible après 30 s"; exit 1; }
echo "$STATUS" | python3 -m json.tool
