#!/usr/bin/env bash
# Crée le replication_user avec le mot de passe de .env
# Tourné après le DDL initial pour éviter de hardcoder le password dans SQL
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Charge .env
if [ ! -f "$PROJECT_ROOT/.env" ]; then
  echo "[ERROR] .env manquant. Lance : make init-env"
  exit 1
fi
set -a
source "$PROJECT_ROOT/.env"
set +a

echo "Création du replication_user..."
docker exec -i stripe-postgres psql -U "$PG_USER" -d "$PG_DB" <<EOF
DO \$\$
BEGIN
    -- Idempotence: CREATE si absent, sinon rotation du mot de passe.
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${PG_REPLICATION_USER}') THEN
        CREATE ROLE ${PG_REPLICATION_USER} WITH REPLICATION LOGIN PASSWORD '${PG_REPLICATION_PASSWORD}';
    ELSE
        ALTER ROLE ${PG_REPLICATION_USER} WITH PASSWORD '${PG_REPLICATION_PASSWORD}';
    END IF;
END \$\$;

-- Droits minimum nécessaires à Debezium pour lire les tables publiées.
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${PG_REPLICATION_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${PG_REPLICATION_USER};
EOF

echo "[OK] replication_user configuré"
