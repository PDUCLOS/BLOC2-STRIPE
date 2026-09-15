#!/usr/bin/env bash
# Génère le fichier .env à partir de .env.example avec des secrets aléatoires
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

if [ -f "$PROJECT_ROOT/.env" ]; then
  echo "[WARN] .env existe déjà, pas de regen (supprime-le si tu veux forcer)"
  exit 0
fi

cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"

# Substitue les mots de passe par des secrets aléatoires
# token_urlsafe(24) donne ~32 chars robustes sans caractères problématiques
# pour la majorité des shells et URI de connexion.
python3 - <<'EOF'
import secrets
import re
from pathlib import Path

p = Path(".env")
content = p.read_text()
replacements = {
    "PG_PASSWORD=": f"PG_PASSWORD={secrets.token_urlsafe(24)}",
    "PG_REPLICATION_PASSWORD=": f"PG_REPLICATION_PASSWORD={secrets.token_urlsafe(24)}",
    "MONGO_PASSWORD=": f"MONGO_PASSWORD={secrets.token_urlsafe(24)}",
    "MONGO_APP_PASSWORD=": f"MONGO_APP_PASSWORD={secrets.token_urlsafe(24)}",
    "REDIS_PASSWORD=": f"REDIS_PASSWORD={secrets.token_urlsafe(24)}",
}
for k, v in replacements.items():
    # Remplace SEULEMENT les valeurs vides après le =
    content = re.sub(rf"^{re.escape(k)}$", v, content, flags=re.MULTILINE)
p.write_text(content)
print("[OK] .env généré avec des secrets aléatoires")
EOF
