#!/usr/bin/env bash
# Script de démo one-shot — démarre tout le pipeline pour la vidéo
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

cd "$PROJECT_ROOT"

echo "═══════════════════════════════════════════════════════════"
echo "  STRIPE POLYGLOT — Démo locale"
echo "═══════════════════════════════════════════════════════════"
echo ""

# 1. Init env si manquant
if [ ! -f .env ]; then
    echo "🔑 Génération du .env..."
    bash scripts/init_env.sh
fi

# 2. venv
if [ ! -d venv ]; then
    echo "🐍 Création du venv Python 3.11..."
    /opt/homebrew/bin/python3.11 -m venv venv
    ./venv/bin/pip install --upgrade pip > /dev/null 2>&1
    ./venv/bin/pip install -r requirements.txt
    ./venv/bin/pip install python-snappy  # pour la décompression Kafka snappy
fi

# Load env
set -a
source .env
set +a

# 3. Démarre les services Docker
echo "🐳 Démarrage des services Docker..."
docker compose --env-file .env up -d postgres redis mongo kafka debezium 2>&1 | tail -3

# 4. Attendre que tous les services soient healthy
echo "⏳ Attente des services healthy..."
for i in {1..30}; do
    HEALTHY=$(docker compose --env-file .env ps --format json 2>/dev/null | grep -c '"Health":"healthy"' || echo 0)
    if [ "$HEALTHY" -ge 4 ]; then
        echo "✅ Services ready"
        break
    fi
    sleep 2
    echo "  ... ($i/30)"
done

# 5. Init Kafka topics + Debezium
echo "📨 Topics Kafka + Debezium connector..."
bash scripts/create_topics.sh > /dev/null 2>&1
bash scripts/postgres_init_roles.sh > /dev/null 2>&1
bash scripts/deploy_debezium.sh > /dev/null 2>&1

# 6. Seed si nécessaire
COUNT=$(./venv/bin/python -c "import psycopg2, os; c=psycopg2.connect(host=os.environ['PG_HOST'], dbname=os.environ['PG_DB'], user=os.environ['PG_USER'], password=os.environ['PG_PASSWORD']); cur=c.cursor(); cur.execute('SELECT count(*) FROM merchants'); print(cur.fetchone()[0])")
if [ "$COUNT" -lt 100 ]; then
    echo "🌱 Seed des données (200 merchants, 5000 customers)..."
    ./venv/bin/python seed/seed_data.py 2>&1 | tail -5
else
    echo "✅ Seed déjà fait ($COUNT merchants)"
fi

# 7. Clean Mongo pour la démo
docker exec stripe-mongo mongosh --quiet -u admin -p "$MONGO_PASSWORD" --authenticationDatabase admin --eval "const db = db.getSiblingDB('stripe_nosql'); db.transaction_logs.deleteMany({}); db.fraud_alerts.deleteMany({}); print('Mongo cleaned');" 2>&1 | tail -2

# 8. Kill anciens process
pkill -9 -f flink_like_job.py 2>/dev/null || true
pkill -9 -f mongo_writer.py 2>/dev/null || true
pkill -9 -f "streamlit run" 2>/dev/null || true
sleep 2

# 9. Lance le pipeline (en background)
echo ""
echo "🚀 Lancement du pipeline..."
echo ""

./venv/bin/python -u producers/flink_like_job.py > /tmp/flink.log 2>&1 &
echo "  → Flink-like job (PID $!)"

./venv/bin/python -u producers/mongo_writer.py > /tmp/mongo.log 2>&1 &
echo "  → Mongo writer (PID $!)"

./venv/bin/streamlit run dashboard/app.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.headless true \
    --browser.gatherUsageStats false > /tmp/streamlit.log 2>&1 &
echo "  → Streamlit dashboard (PID $!)"

sleep 3

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  ✅ DÉMO PRÊTE !"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  📊 Dashboard :     http://localhost:8501"
echo "  🔌 Kafka Connect : http://localhost:8083"
echo ""
echo "  Pour lancer le producer de transactions :"
echo "    ./venv/bin/python producers/transaction_producer.py --rate 3"
echo ""
echo "  Pour lancer le test E2E :"
echo "    ./venv/bin/python tests/test_e2e.py"
echo ""
echo "  Pour arrêter tout :"
echo "    pkill -9 -f flink_like_job.py"
echo "    pkill -9 -f mongo_writer.py"
echo "    pkill -9 -f 'streamlit run'"
echo "    docker compose down"
echo ""
echo "═══════════════════════════════════════════════════════════"
