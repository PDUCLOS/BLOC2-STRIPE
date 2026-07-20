#!/usr/bin/env python3
"""
Stripe Polyglot — Dashboard de démo
Streamlit live : KPIs fraude, transactions, alertes MongoDB, vélocité Redis

Usage : make dashboard   (ou ./venv/bin/python -m streamlit run dashboard/app.py)
URL   : http://localhost:8501
"""
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient

# ── Chargement .env ────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

PG_CONFIG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", 5432)),
    dbname=os.environ.get("PG_DB", "stripe_oltp"),
    user=os.environ.get("PG_USER", "stripe_app"),
    password=os.environ.get("PG_PASSWORD", ""),
)
REDIS_CONFIG = dict(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    password=os.environ.get("REDIS_PASSWORD") or None,
    decode_responses=True,
    socket_timeout=2,
)
MONGO_CONFIG = dict(
    host=os.environ.get("MONGO_HOST", "localhost"),
    port=int(os.environ.get("MONGO_PORT", 27017)),
    user=os.environ.get("MONGO_USER", "admin"),
    password=os.environ.get("MONGO_PASSWORD", ""),
    db=os.environ.get("MONGO_DB", "stripe_nosql"),
)
FRAUD_THRESHOLD = float(os.environ.get("FRAUD_SCORE_THRESHOLD", 0.85))

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stripe Polyglot — Dashboard",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS personnalisé ───────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card { background: #1e2130; border-radius: 8px; padding: 1rem; border-left: 4px solid #635BFF; }
    .alert-card  { background: #2a1f1f; border-radius: 6px; padding: 0.5rem 1rem; border-left: 4px solid #ff4444; margin: 4px 0; color: #ffffff; }
    .ok-card     { background: #1a2a1f; border-radius: 6px; padding: 0.5rem 1rem; border-left: 4px solid #00aa44; margin: 4px 0; color: #ffffff; }
    h1 { color: #635BFF; }
    .stMetric label { font-size: 0.85rem !important; }
</style>
""", unsafe_allow_html=True)

# ── Connexions (cachées 5 s pour le live refresh) ─────────────────────────────
@st.cache_resource(ttl=3)
def get_pg():
    """Initialise et met en cache la connexion à PostgreSQL.
    
    Returns:
        psycopg2.extensions.connection ou None si échec.
    """
    try:
        return psycopg2.connect(**PG_CONFIG)
    except Exception:
        return None

@st.cache_resource(ttl=3)
def get_redis():
    """Initialise et met en cache la connexion à Redis.
    
    Returns:
        redis.Redis ou None si échec.
    """
    try:
        r = redis.Redis(**REDIS_CONFIG)
        r.ping()
        return r
    except Exception:
        return None

@st.cache_resource(ttl=3)
def get_mongo():
    """Initialise et met en cache la connexion à MongoDB.
    
    Returns:
        pymongo.collection.Collection ou None si échec.
    """
    try:
        if MONGO_CONFIG["user"] and MONGO_CONFIG["password"]:
            uri = f"mongodb://{MONGO_CONFIG['user']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        else:
            uri = f"mongodb://{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        client = MongoClient(uri, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        return client[MONGO_CONFIG["db"]]
    except Exception:
        return None

# ── Fonctions de fetch ─────────────────────────────────────────────────────────
def pg_query(sql, params=None):
    """Exécute une requête SQL sur PostgreSQL et retourne un DataFrame Pandas.
    
    Args:
        sql (str): Requête SQL à exécuter.
        params (tuple, optional): Paramètres optionnels pour la requête.
        
    Returns:
        pd.DataFrame: Résultats de la requête, ou un DataFrame vide en cas d'erreur.
    """
    conn = get_pg()
    if conn is None:
        return pd.DataFrame()
    try:
        if conn.closed:
            return pd.DataFrame()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()

def kpis_from_pg():
    """Récupère les KPIs globaux depuis PostgreSQL.
    
    Returns:
        dict: Dictionnaire contenant le total des transactions, le revenu, les fraudes, etc.
    """
    df = pg_query("""
        SELECT
            COUNT(*)                                              AS total_txns,
            COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1h') AS txns_1h,
            COALESCE(SUM(amount) FILTER (WHERE status='succeeded') / 100.0, 0) AS revenue_eur,
            COUNT(*) FILTER (WHERE fraud_score >= %s)            AS fraud_count,
            COALESCE(AVG(fraud_score) FILTER (WHERE fraud_score IS NOT NULL), 0) AS avg_fraud_score,
            COUNT(*) FILTER (WHERE status = 'succeeded')         AS succeeded,
            COUNT(*) FILTER (WHERE status = 'failed')            AS failed
        FROM transactions
    """, (FRAUD_THRESHOLD,))
    if df.empty:
        return {}
    row = df.iloc[0]
    total = int(row["total_txns"]) if row["total_txns"] else 1
    fraud = int(row["fraud_count"]) if row["fraud_count"] else 0
    return {
        "total": total,
        "txns_1h": int(row["txns_1h"] or 0),
        "revenue_eur": float(row["revenue_eur"] or 0),
        "fraud_count": fraud,
        "fraud_rate": round(fraud / total * 100, 2),
        "avg_score": float(row["avg_fraud_score"] or 0),
        "succeeded": int(row["succeeded"] or 0),
        "failed": int(row["failed"] or 0),
    }

def txn_over_time():
    """Récupère l'évolution des transactions et fraudes sur les 30 dernières minutes.
    
    Returns:
        pd.DataFrame: Données agrégées par minute.
    """
    return pg_query("""
        SELECT
            DATE_TRUNC('minute', created_at) AS minute,
            COUNT(*) AS count,
            COUNT(*) FILTER (WHERE fraud_score >= %s) AS fraud_count,
            COALESCE(SUM(amount)/100.0, 0) AS volume_eur
        FROM transactions
        WHERE created_at >= NOW() - INTERVAL '30 minutes'
        GROUP BY 1 ORDER BY 1
    """, (FRAUD_THRESHOLD,))

def fraud_by_country():
    """Récupère le top 10 des pays avec le plus de fraudes détectées.
    
    Returns:
        pd.DataFrame: Données des fraudes par pays.
    """
    return pg_query("""
        SELECT ip_country, COUNT(*) AS fraud_count
        FROM transactions
        WHERE fraud_score >= %s AND ip_country IS NOT NULL
        GROUP BY ip_country ORDER BY fraud_count DESC LIMIT 10
    """, (FRAUD_THRESHOLD,))

def top_merchants():
    """Récupère le top 8 des marchands par volume d'affaires (GMV).
    
    Returns:
        pd.DataFrame: Données du top marchands.
    """
    return pg_query("""
        SELECT
            m.name,
            COUNT(t.txn_id) AS txn_count,
            COALESCE(SUM(t.amount) FILTER (WHERE t.status='succeeded')/100.0, 0) AS gmv
        FROM merchants m
        JOIN transactions t ON t.merchant_id = m.merchant_id
        GROUP BY m.merchant_id, m.name ORDER BY gmv DESC LIMIT 8
    """)

def fraud_alerts_mongo(limit=20):
    """Récupère les alertes de fraude les plus récentes depuis MongoDB.
    
    Args:
        limit (int): Nombre maximum d'alertes à récupérer.
        
    Returns:
        list[dict]: Liste des documents d'alerte.
    """
    db = get_mongo()
    if db is None:
        return []
    try:
        return list(db.fraud_alerts.find(
            {}, {"_id": 0},
            sort=[("created_at", -1)],
            limit=limit
        ))
    except Exception:
        return []

def redis_stats(customer_ids):
    """Récupère les velocities Redis pour quelques clients."""
    r = get_redis()
    if r is None or not customer_ids:
        return {}
    out = {}
    for cid in customer_ids[:5]:
        try:
            v1h = r.zcard(f"v1h_{cid}")
            v24h = r.zcard(f"v24h_{cid}")
            out[str(cid)[:8]] = {"v1h": v1h, "v24h": v24h}
        except Exception:
            pass
    return out

def recent_suspicious():
    """Récupère les 15 dernières transactions suspectes (score >= 0.6) depuis PostgreSQL.
    
    Returns:
        pd.DataFrame: Données des transactions suspectes.
    """
    return pg_query("""
        SELECT t.txn_id, t.amount/100.0 AS amount_eur, t.currency,
               t.fraud_score, t.ip_country, t.device_type, t.created_at
        FROM transactions t
        WHERE t.fraud_score >= 0.6
        ORDER BY t.created_at DESC LIMIT 15
    """)

# ── SIDEBAR ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/b/ba/Stripe_Logo%2C_revised_2016.svg", width=120)
    st.title("Stripe Polyglot")
    st.caption("Certification AIA RNCP41993 — Bloc 2")
    st.divider()

    refresh = st.slider("Auto-refresh (s)", 1, 30, 5)
    st.caption(f"Prochain refresh dans {refresh}s")
    st.divider()

    # Status des services
    st.subheader("🔌 Services")
    pg_ok = get_pg() is not None and not get_pg().closed
    redis_ok = get_redis() is not None
    mongo_ok = get_mongo() is not None

    st.markdown(f"{'🟢' if pg_ok else '🔴'} **PostgreSQL** (OLTP)")
    st.markdown(f"{'🟢' if redis_ok else '🔴'} **Redis** (feature store)")
    st.markdown(f"{'🟢' if mongo_ok else '🔴'} **MongoDB** (logs/alertes)")
    st.divider()

    st.caption("Stack : PostgreSQL 16 · MongoDB 7 · Kafka KRaft · Debezium 2.6 · Redis 7")
    st.caption("Pipeline : CDC → Kafka → Flink-like scorer → MongoDB")

# ── HEADER ─────────────────────────────────────────────────────────────────────
st.title("💳 Stripe Polyglot — Dashboard Temps Réel")
st.caption(f"Dernière mise à jour : {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")

# ── KPIs ───────────────────────────────────────────────────────────────────────
kpis = kpis_from_pg()
if kpis:
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("🔄 Transactions totales", f"{kpis['total']:,}")
    col2.metric("⚡ Txns (1h)", f"{kpis['txns_1h']:,}")
    col3.metric("💶 Revenus", f"{kpis['revenue_eur']:,.0f} €")
    col4.metric("⚠️ Alertes fraude", f"{kpis['fraud_count']:,}", delta=f"{kpis['fraud_rate']}% du volume", delta_color="inverse")
    col5.metric("📊 Score fraude moyen", f"{kpis['avg_score']:.3f}")
else:
    st.warning("⏳ PostgreSQL pas encore disponible — lance `make up` puis `make seed`")

st.divider()

# ── CHARTS ROW 1 ───────────────────────────────────────────────────────────────
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📈 Transactions & Fraude — 30 dernières minutes")
    df_time = txn_over_time()
    if not df_time.empty and "minute" in df_time.columns:
        df_time["minute"] = pd.to_datetime(df_time["minute"])
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_time["minute"], y=df_time["count"],
            name="Total transactions", line=dict(color="#635BFF", width=2),
            fill="tozeroy", fillcolor="rgba(99,91,255,0.08)"
        ))
        fig.add_trace(go.Scatter(
            x=df_time["minute"], y=df_time["fraud_count"],
            name="Transactions frauduleuses", line=dict(color="#ff4444", width=2),
            fill="tozeroy", fillcolor="rgba(255,68,68,0.08)"
        ))
        fig.update_layout(
            height=280, margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=1.1),
            xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333")
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("En attente de données... Lance le producer : `make producer`")

with col_right:
    st.subheader("🌍 Fraude par pays")
    df_geo = fraud_by_country()
    if not df_geo.empty:
        fig_geo = px.bar(
            df_geo, x="fraud_count", y="ip_country", orientation="h",
            color="fraud_count",
            color_continuous_scale=[[0, "#635BFF"], [0.5, "#ff8800"], [1, "#ff4444"]],
            height=280
        )
        fig_geo.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_showscale=False,
            yaxis=dict(gridcolor="#333"), xaxis=dict(gridcolor="#333")
        )
        st.plotly_chart(fig_geo, use_container_width=True)
    else:
        st.info("Pas encore d'alertes fraude")

st.divider()

# ── CHARTS ROW 2 ───────────────────────────────────────────────────────────────
col_merch, col_alerts = st.columns([1, 1])

with col_merch:
    st.subheader("🏪 Top marchands (GMV)")
    df_merch = top_merchants()
    if not df_merch.empty:
        fig_merch = px.bar(
            df_merch, x="gmv", y="name", orientation="h",
            text_auto=".0f",
            color="txn_count",
            color_continuous_scale=[[0, "#635BFF"], [1, "#9d97ff"]],
            height=300
        )
        fig_merch.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_showscale=False,
            yaxis=dict(gridcolor="#333"), xaxis=dict(gridcolor="#333")
        )
        st.plotly_chart(fig_merch, use_container_width=True)
    else:
        st.info("Aucun marchand — lance `make seed`")

with col_alerts:
    st.subheader("🚨 Alertes MongoDB (review/block)")
    alerts = fraud_alerts_mongo(10)
    if alerts:
        for a in alerts:
            decision = a.get("decision", "?")
            color = "🔴" if decision == "block" else "🟡"
            amount = (a.get("amount") or 0) / 100
            score = a.get("fraud_score", 0)
            country = a.get("ip_country", "?")
            rules = ", ".join(a.get("rules_triggered") or []) or "—"
            ts = a.get("created_at", "")
            if hasattr(ts, "strftime"):
                ts = ts.strftime("%H:%M:%S")
            st.markdown(
                f"""<div class="{'alert-card' if decision == 'block' else 'ok-card'}">
                {color} <b>{decision.upper()}</b> · {amount:.2f}€ · {country} · score={score:.3f}<br>
                <small>Règles : {rules} · {ts}</small>
                </div>""",
                unsafe_allow_html=True
            )
    else:
        st.info("Aucune alerte dans MongoDB — le pipeline est-il actif ?")

st.divider()

# ── TRANSACTIONS SUSPECTES ─────────────────────────────────────────────────────
st.subheader("🔍 Transactions suspectes récentes (score ≥ 0.6)")
df_sus = recent_suspicious()
if not df_sus.empty:
    # Coloriser le score
    def color_score(val):
        if val is None:
            return ""
        val = float(val)
        if val >= FRAUD_THRESHOLD:
            return "background-color: #3d1515; color: #ff6666"
        elif val >= 0.6:
            return "background-color: #3d2d10; color: #ffaa44"
        return ""

    st.dataframe(
        df_sus.rename(columns={
            "txn_id": "ID Transaction", "amount_eur": "Montant (€)",
            "currency": "Devise", "fraud_score": "Score fraude",
            "ip_country": "Pays IP", "device_type": "Device",
            "created_at": "Horodatage"
        }),
        use_container_width=True,
        height=300,
    )
else:
    st.info("Aucune transaction suspecte pour l'instant")

st.divider()

# ── ARCHITECTURE ───────────────────────────────────────────────────────────────
with st.expander("🏗️ Architecture — Pipeline de traitement", expanded=False):
    st.markdown("""
```
API Stripe
    ↓
PostgreSQL 16 (OLTP · ACID · 3NF)
    ↓ WAL
Debezium CDC
    ↓
Apache Kafka (stripe.public.transactions)
    ↓
Flink-like Scorer (Python · Redis feature store)
    ├─ fraud_score → UPDATE transactions.fraud_score
    ├─ stripe.payments.events (toutes les txns)
    └─ stripe.fraud.alerts (review/block)
              ↓
         MongoDB Writer
              ├─ transaction_logs (TTL 90j · RGPD)
              └─ fraud_alerts

Airflow (batch · 02:00 UTC)
    ↓
Snowflake OLAP (star schema · fact_transactions + 5 dims)
```

**Garanties** : PCI-DSS v4.0 · RGPD · TLS 1.3 · RBAC · AWS KMS  
**Latence fraude** : < 50ms bout en bout  
**Throughput** : 10 000+ transactions/s (Kafka · 12 partitions)
    """)

# ── FOOTER + AUTO-REFRESH ──────────────────────────────────────────────────────
st.caption("Certification AIA RNCP41993 · Bloc 2 · Patrice Duclos · 2026")

# Auto-refresh via rerun
time.sleep(refresh)
st.rerun()
