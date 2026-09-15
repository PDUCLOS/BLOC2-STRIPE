#!/usr/bin/env python3
"""
Stripe Polyglot — Dashboard de démo
Streamlit live : KPIs fraude, transactions, alertes MongoDB, vélocité Redis

Usage : make dashboard   (ou ./venv/bin/python -m streamlit run dashboard/app.py)
URL   : http://localhost:8501
"""
import hashlib
import hmac
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
# Le dashboard tourne depuis dashboard/, donc on remonte à la racine du projet
# pour que _env trouve le .env quel que soit le répertoire d'appel de streamlit.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401 — l'import seul déclenche le chargement du .env

# Config des 3 datastores lues depuis l'environnement, avec des valeurs par défaut
# alignées sur docker-compose.yml pour que `streamlit run` marche aussi hors Docker
# (connexion directe aux ports exposés sur localhost).
#
# Le dashboard n'a besoin que d'un accès lecture seule sans les colonnes
# sensibles de payment_methods (cf. docs/SECURITY_COMPLIANCE_PLAN.md §3.2) :
# utilise analytics_reader si PG_ANALYTICS_USER/PASSWORD sont renseignés dans
# .env, sinon retombe sur PG_USER (stripe_app) pour ne pas casser une install
# existante où le rôle analytics_reader n'a pas encore été créé.
_PG_USER = os.environ.get("PG_ANALYTICS_USER") or os.environ.get("PG_USER", "stripe_app")
_PG_PASSWORD = os.environ.get("PG_ANALYTICS_PASSWORD") or os.environ.get("PG_PASSWORD", "")
PG_CONFIG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", 5432)),
    dbname=os.environ.get("PG_DB", "stripe_oltp"),
    user=_PG_USER,
    password=_PG_PASSWORD,
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
# Doit rester identique au seuil utilisé côté scorer (flink/fraud_scoring_job.py,
# producers/flink_like_job.py) : sinon le dashboard et le moteur de décision
# ne comptent pas les mêmes transactions comme frauduleuses.

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stripe Polyglot — Dashboard",
    page_icon=":credit_card:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Authentification ──────────────────────────────────────────────────────────
# Contrôle d'accès minimal mais réel avant toute donnée : le dashboard expose
# des montants, pays, scores de fraude par client — pas de contenu métier
# sans passer par ici. Le mot de passe n'est jamais comparé/stocké en clair,
# seul son hash SHA-256 vit dans .env (généré par make init-env, cf.
# scripts/init_env.sh) ; comparaison en temps constant (hmac.compare_digest)
# pour ne pas fuiter d'information via le timing de la requête.
DASHBOARD_USERNAME = os.environ.get("DASHBOARD_USERNAME", "")
DASHBOARD_PASSWORD_HASH = os.environ.get("DASHBOARD_PASSWORD_HASH", "")
MAX_LOGIN_ATTEMPTS = int(os.environ.get("DASHBOARD_MAX_LOGIN_ATTEMPTS", 5))
LOCKOUT_SECONDS = int(os.environ.get("DASHBOARD_LOCKOUT_SECONDS", 60))


def _check_credentials(username: str, password: str) -> bool:
    """Compare en temps constant pour ne pas laisser fuiter, via la durée de
    réponse, la position du premier caractère qui diffère (timing attack)."""
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    user_ok = hmac.compare_digest(username, DASHBOARD_USERNAME)
    pass_ok = hmac.compare_digest(password_hash, DASHBOARD_PASSWORD_HASH)
    return user_ok and pass_ok


def require_login():
    """Bloque tout rendu tant que l'utilisateur n'est pas authentifié.
    Verrouillage temporaire après N échecs pour ralentir un bruteforce —
    ça ne remplace pas un vrai IAM en prod, mais démontre le principe pour
    un accès qui n'a, ici, qu'un seul compte de démo."""
    if st.session_state.get("authenticated"):
        return

    if not DASHBOARD_PASSWORD_HASH:
        st.error(
            "Authentification non configurée : DASHBOARD_PASSWORD_HASH est vide dans .env.\n\n"
            "Lance `make init-env` (génère un mot de passe aléatoire et affiche son hash une fois)."
        )
        st.stop()

    st.session_state.setdefault("login_attempts", 0)
    st.session_state.setdefault("locked_until", 0.0)

    now = time.time()
    locked_remaining = st.session_state["locked_until"] - now
    if locked_remaining > 0:
        st.title(":credit_card: Stripe Polyglot — Dashboard")
        st.error(f"Trop de tentatives échouées. Réessaie dans {int(locked_remaining) + 1}s.")
        st.stop()

    st.title(":credit_card: Stripe Polyglot — Dashboard")
    st.caption("Accès restreint — données financières et de fraude.")
    with st.form("login_form"):
        username = st.text_input("Utilisateur")
        password = st.text_input("Mot de passe", type="password")
        submitted = st.form_submit_button("Se connecter")

    if submitted:
        if _check_credentials(username, password):
            st.session_state["authenticated"] = True
            st.session_state["login_attempts"] = 0
            st.rerun()
        else:
            st.session_state["login_attempts"] += 1
            remaining = MAX_LOGIN_ATTEMPTS - st.session_state["login_attempts"]
            if remaining <= 0:
                st.session_state["locked_until"] = now + LOCKOUT_SECONDS
                st.session_state["login_attempts"] = 0
                st.error(f"Trop de tentatives échouées. Verrouillé {LOCKOUT_SECONDS}s.")
            else:
                st.error(f"Identifiants incorrects ({remaining} tentative(s) restante(s)).")
    st.stop()


require_login()

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

# ── Connexions (cachées 3 s pour le live refresh) ─────────────────────────────
# Cache court pour limiter les reconnexions à chaque rerun Streamlit, sans garder
# des états obsolètes trop longtemps pendant la démo en temps réel.
@st.cache_resource(ttl=3)
def get_pg():
    """None si Postgres n'est pas dispo — l'UI affiche l'état "en attente" plutôt que de crasher."""
    try:
        return psycopg2.connect(**PG_CONFIG)
    except Exception:
        return None

@st.cache_resource(ttl=3)
def get_redis():
    """Idem get_pg, mais pour Redis (avec ping pour vérifier que la connexion tient vraiment)."""
    try:
        r = redis.Redis(**REDIS_CONFIG)
        r.ping()
        return r
    except Exception:
        return None

@st.cache_resource(ttl=3)
def get_mongo():
    """Construit l'URI selon qu'on a des credentials ou pas, ping pour valider, renvoie la db directement."""
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
        # En cas de fermeture côté serveur, on évite une exception bruyante
        # et on laisse l'UI afficher un état "en attente".
        if conn.closed:
            return pd.DataFrame()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3)
def kpis_from_pg():
    """Récupère les KPIs globaux depuis PostgreSQL.
    
    Returns:
        dict: Dictionnaire contenant le total des transactions, le revenu, les fraudes, etc.
    """
    # Une seule requête sur toute la table transactions, avec des agrégats FILTER
    # (Postgres) pour calculer plusieurs compteurs conditionnels en un seul scan
    # plutôt que 7 requêtes séparées — plus économe, un aller-retour réseau unique.
    df = pg_query("""
        SELECT
            -- Nombre total de lignes, toutes transactions confondues (dénominateur du taux de fraude).
            COUNT(*)                                              AS total_txns,
            -- Transactions créées dans la dernière heure glissante (indicateur d'activité live).
            COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1h') AS txns_1h,
            -- Somme des montants encaissés (status='succeeded' uniquement, pas les échecs/refus).
            -- /100.0 car amount est stocké en centimes (BIGINT) ; COALESCE évite un NULL si aucune ligne.
            COALESCE(SUM(amount) FILTER (WHERE status='succeeded') / 100.0, 0) AS revenue_eur,
            -- Nombre de transactions dont le score dépasse le seuil de blocage fraude.
            COUNT(*) FILTER (WHERE fraud_score >= %s)            AS fraud_count,
            -- Score de fraude moyen, calculé uniquement sur les lignes déjà scorées
            -- (le scoring est asynchrone : une transaction fraîche peut avoir fraud_score IS NULL).
            COALESCE(AVG(fraud_score) FILTER (WHERE fraud_score IS NOT NULL), 0) AS avg_fraud_score,
            COUNT(*) FILTER (WHERE status = 'succeeded')         AS succeeded,
            COUNT(*) FILTER (WHERE status = 'failed')            AS failed
        FROM transactions
    """, (FRAUD_THRESHOLD,))
    if df.empty:
        return {}
    row = df.iloc[0]
    # Protection anti-division par zéro pour le calcul du taux de fraude.
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

@st.cache_data(ttl=3)
def txn_over_time():
    """Récupère l'évolution des transactions et fraudes sur les 30 dernières minutes.
    
    Returns:
        pd.DataFrame: Données agrégées par minute.
    """
    # Fenêtre glissante courte (30 min) : suffisante pour visualiser la dynamique live
    # sans surcharger le rendu Plotly pendant la démo.
    return pg_query("""
        SELECT
            -- Regroupe les horodatages à la minute près pour produire une série
            -- temporelle lisible (sinon une ligne par transaction, illisible en graphe).
            DATE_TRUNC('minute', created_at) AS minute,
            COUNT(*) AS count,
            -- Sous-compte des transactions frauduleuses dans la même minute
            -- (superposé au total dans le graphe pour comparer les deux courbes).
            COUNT(*) FILTER (WHERE fraud_score >= %s) AS fraud_count,
            COALESCE(SUM(amount)/100.0, 0) AS volume_eur
        FROM transactions
        -- Ne filtre que les 30 dernières minutes : borne le volume de lignes
        -- remontées côté client à chaque refresh streamlit.
        WHERE created_at >= NOW() - INTERVAL '30 minutes'
        -- GROUP BY 1 / ORDER BY 1 = par position (colonne "minute") plutôt que
        -- retaper l'expression DATE_TRUNC ; ORDER BY garantit une courbe chronologique.
        GROUP BY 1 ORDER BY 1
    """, (FRAUD_THRESHOLD,))

@st.cache_data(ttl=3)
def fraud_by_country():
    """Récupère le top 10 des pays avec le plus de fraudes détectées.
    
    Returns:
        pd.DataFrame: Données des fraudes par pays.
    """
    return pg_query("""
        SELECT ip_country, COUNT(*) AS fraud_count
        FROM transactions
        -- Ne garde que les transactions déjà au-dessus du seuil de fraude ;
        -- ip_country IS NOT NULL exclut les transactions sans géolocalisation résolue.
        WHERE fraud_score >= %s AND ip_country IS NOT NULL
        -- Un pays par groupe, trié par nombre de fraudes décroissant, tronqué au top 10
        -- pour tenir dans le graphe en barres horizontales du dashboard.
        GROUP BY ip_country ORDER BY fraud_count DESC LIMIT 10
    """, (FRAUD_THRESHOLD,))

@st.cache_data(ttl=3)
def top_merchants():
    """Récupère le top 8 des marchands par volume d'affaires (GMV).
    
    Returns:
        pd.DataFrame: Données du top marchands.
    """
    return pg_query("""
        SELECT
            m.name,
            -- Nombre total de transactions du marchand, succeeded ou non
            -- (contrairement au gmv ci-dessous qui ne compte que les paiements réussis).
            COUNT(t.txn_id) AS txn_count,
            -- GMV = Gross Merchandise Value : somme des montants encaissés avec succès
            -- uniquement (les échecs ne génèrent pas de revenu réel pour le marchand).
            COALESCE(SUM(t.amount) FILTER (WHERE t.status='succeeded')/100.0, 0) AS gmv
        FROM merchants m
        -- INNER JOIN volontaire (pas LEFT) : un marchand sans transaction n'a pas sa
        -- place dans un classement "top marchands par volume d'affaires".
        JOIN transactions t ON t.merchant_id = m.merchant_id
        -- Regroupé par marchand (merchant_id + name, la clé + son libellé) puis
        -- trié par GMV décroissant, limité au top 8 pour l'affichage en graphe.
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
            {},                     # pas de filtre : toutes les alertes (review + block confondus)
            {"_id": 0},             # projection : exclut l'ObjectId Mongo, inutile côté UI
            sort=[("created_at", -1)],  # plus récentes d'abord
            limit=limit
        ))
    except Exception:
        return []

def redis_stats(customer_ids):
    """Récupère les compteurs de vélocité (nb transactions 1h/24h) depuis Redis.

    Ces compteurs sont les mêmes features (sorted sets `v1h_<id>`/`v24h_<id>`)
    que celles utilisées par le scorer fraude en temps réel — affichées ici
    à titre illustratif pour la démo, pas pour recalculer un score.

    Args:
        customer_ids (list): IDs clients à interroger (échantillonnés à 5 max).

    Returns:
        dict: {customer_id_tronqué: {"v1h": int, "v24h": int}}.
    """
    r = get_redis()
    if r is None or not customer_ids:
        return {}
    out = {}
    # On limite volontairement l'échantillon pour garder une latence UI stable.
    for cid in customer_ids[:5]:
        try:
            v1h = r.zcard(f"v1h_{cid}")
            v24h = r.zcard(f"v24h_{cid}")
            out[str(cid)[:8]] = {"v1h": v1h, "v24h": v24h}
        except Exception:
            pass
    return out

@st.cache_data(ttl=3)
def recent_suspicious():
    """Récupère les 15 dernières transactions suspectes (score >= 0.6) depuis PostgreSQL.
    
    Returns:
        pd.DataFrame: Données des transactions suspectes.
    """
    return pg_query("""
        SELECT t.txn_id, t.amount/100.0 AS amount_eur, t.currency,
               t.fraud_score, t.ip_country, t.device_type, t.created_at
        FROM transactions t
        -- Seuil 0.6 = REVIEW_THRESHOLD (pas FRAUD_THRESHOLD 0.85) : on veut voir
        -- ici toute la zone "à surveiller", pas seulement les transactions bloquées.
        WHERE t.fraud_score >= 0.6
        -- Les plus récentes en premier, limité à 15 lignes pour la table du dashboard.
        ORDER BY t.created_at DESC LIMIT 15
    """)

def ml_monitoring_latest():
    """Récupère le dernier cycle de monitoring écrit par le service ml-monitor.

    Returns:
        dict | None: Document ml_monitoring le plus récent, ou None si
        MongoDB est indisponible ou si le service ml-monitor n'a pas encore
        tourné (collection vide).
    """
    db = get_mongo()
    if db is None:
        return None
    try:
        return db.ml_monitoring.find_one(sort=[("checked_at", -1)])
    except Exception:
        return None

def ml_monitoring_history(limit=50):
    """Récupère l'historique des derniers cycles de monitoring, pour le graphe d'évolution.

    Args:
        limit (int): Nombre max de cycles à récupérer.

    Returns:
        list[dict]: Documents ml_monitoring, du plus récent au plus ancien.
    """
    db = get_mongo()
    if db is None:
        return []
    try:
        return list(db.ml_monitoring.find({}, sort=[("checked_at", -1)], limit=limit))
    except Exception:
        return []

def fraud_score_by_model_version(limit=300):
    """Récupère les scores de fraude récents groupés par version de modèle.

    Permet de visualiser si le modèle ML (xgboost-v1) et le moteur à règles
    (rule-based-v1) produisent des distributions de score différentes —
    utile pour repérer un des deux moteurs qui déclencherait trop/pas assez.

    Args:
        limit (int): Nombre max d'alertes à récupérer.

    Returns:
        pd.DataFrame: Colonnes fraud_score, model_version.
    """
    db = get_mongo()
    if db is None:
        return pd.DataFrame()
    try:
        docs = list(db.fraud_alerts.find(
            {}, {"fraud_score": 1, "model_version": 1, "_id": 0},
            sort=[("created_at", -1)], limit=limit
        ))
        return pd.DataFrame(docs)
    except Exception:
        return pd.DataFrame()

# ── SIDEBAR ────────────────────────────────────────────────────────────────────
with st.sidebar:
    # Texte stylé plutôt qu'un logo chargé depuis une URL externe : la démo
    # ne doit pas dépendre du réseau pendant la soutenance (et évite de
    # embarquer un asset de marque tiers dans le repo).
    st.markdown('<div style="font-size:1.8rem;font-weight:700;color:#635BFF;">Stripe</div>', unsafe_allow_html=True)
    st.title("Stripe Polyglot")
    st.caption("Certification AIA RNCP41993 — Bloc 2")
    st.caption(f"Connecté : **{DASHBOARD_USERNAME}**")
    if st.button("Se déconnecter"):
        st.session_state["authenticated"] = False
        st.rerun()
    st.divider()

    refresh = st.slider("Auto-refresh (s)", 1, 30, 5)
    st.caption(f"Prochain refresh dans {refresh}s")
    st.divider()

    # Status des services
    st.subheader("Services")
    # Vérification "best effort" de disponibilité des services pour feedback instantané.
    # get_pg() est rappelé deux fois mais reste bon marché : @st.cache_resource(ttl=3)
    # renvoie la même connexion tant que le cache n'a pas expiré.
    pg_ok = get_pg() is not None and not get_pg().closed
    redis_ok = get_redis() is not None
    mongo_ok = get_mongo() is not None

    st.markdown(f"**[{'OK' if pg_ok else 'DOWN'}]** **PostgreSQL** (OLTP)")
    st.markdown(f"**[{'OK' if redis_ok else 'DOWN'}]** **Redis** (feature store)")
    st.markdown(f"**[{'OK' if mongo_ok else 'DOWN'}]** **MongoDB** (logs/alertes)")
    st.divider()

    st.caption("Stack : PostgreSQL 16 · MongoDB 7 · Kafka KRaft · Debezium 2.6 · Redis 7")
    st.caption("Pipeline : CDC → Kafka → Flink-like scorer → MongoDB")

# ── HEADER ─────────────────────────────────────────────────────────────────────
st.title("Stripe Polyglot — Dashboard Temps Réel")
st.caption(f"Dernière mise à jour : {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")

tab_overview, tab_ml = st.tabs(["Vue d'ensemble", "Performance ML"])

with tab_overview:
    # ── KPIs ───────────────────────────────────────────────────────────────────────
    # NOTE SOUTENANCE : commencer par ces 5 tuiles pour poser la valeur métier.
    # Message recommandé : volume traité, revenu, puis exposition au risque fraude.
    # Cette séquence évite de "plonger" trop tôt dans la technique.
    kpis = kpis_from_pg()
    if kpis:
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Transactions totales", f"{kpis['total']:,}")
        col2.metric("Txns (1h)", f"{kpis['txns_1h']:,}")
        col3.metric("Revenus", f"{kpis['revenue_eur']:,.0f} €")
        col4.metric("Alertes fraude", f"{kpis['fraud_count']:,}", delta=f"{kpis['fraud_rate']}% du volume", delta_color="inverse")
        col5.metric("Score fraude moyen", f"{kpis['avg_score']:.3f}")
    else:
        st.warning("PostgreSQL pas encore disponible — lance `make up` puis `make seed`")

    st.divider()

    # ── CHARTS ROW 1 ───────────────────────────────────────────────────────────────
    # NOTE SOUTENANCE : ici on prouve le "temps réel".
    # Graphe gauche = dynamique minute par minute ; graphe droit = segmentation géographique
    # utile pour expliquer la détection d'anomalies par zone.
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("Transactions & Fraude — 30 dernières minutes")
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
        st.subheader("Fraude par pays")
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
    # NOTE SOUTENANCE : ce bloc relie performance commerciale et contrôle du risque.
    # "Top marchands" montre l'activité business ; "Alertes MongoDB" montre la réaction
    # opérationnelle du moteur fraude (review/block) sur les mêmes flux.
    col_merch, col_alerts = st.columns([1, 1])

    with col_merch:
        st.subheader("Top marchands (GMV)")
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
        st.subheader("Alertes MongoDB (review/block)")
        alerts = fraud_alerts_mongo(10)
        if alerts:
            for a in alerts:
                decision = a.get("decision", "?")
                # Mapping visuel simple pour différencier immédiatement review vs block.
                color = "[BLOCK]" if decision == "block" else "[REVIEW]"
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
    # NOTE SOUTENANCE : terminer par cette table pour la traçabilité transactionnelle.
    # Elle permet d'illustrer qu'une alerte est explicable (score, pays, device, horodatage),
    # ce qui renforce le discours conformité/auditabilité.
    st.subheader("Transactions suspectes récentes (score ≥ 0.6)")
    df_sus = recent_suspicious()
    if not df_sus.empty:
        # Conserve la logique de coloration pour un futur .style.applymap ; non activé
        # ici pour privilégier un affichage Streamlit stable et lisible en démo.
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
    # NOTE SOUTENANCE : ouvrir cet expander en fin de démo pour reconnecter les visuels
    # au pipeline complet (OLTP -> CDC -> Kafka -> scoring -> NoSQL -> OLAP).
    with st.expander("Architecture — Pipeline de traitement", expanded=False):
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


with tab_ml:
    # ── ONGLET PERFORMANCE ML ───────────────────────────────────────────────
    # NOTE SOUTENANCE : cet onglet répond à l'exigence "monitoring de la
    # performance du modèle ML" — alimenté par le service ml-monitor
    # (Evidently pour le drift, sklearn pour precision/recall) qui tourne en
    # continu et déclenche un réentraînement automatique en cas de dérive
    # (cf. docs/ML_INTEGRATION_STRATEGY.md).
    st.subheader("Performance du modèle ML")
    st.caption("Suivi drift + précision de xgboost-v1, alimenté par le service ml-monitor (Evidently + MLflow)")

    latest = ml_monitoring_latest()
    if latest is None:
        st.info("Aucune donnée de monitoring pour l'instant — le service ml-monitor tourne-t-il ? "
                "(`docker compose up -d ml-monitor`)")
    else:
        status = latest.get("status", "?")
        drift = latest.get("drift") or {}
        perf = latest.get("performance") or {}

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Statut", status.upper())
        col2.metric("Drift (part colonnes)", f"{drift.get('drift_share', 0):.2f}")
        col3.metric("Recall live", f"{perf['recall']:.2f}" if perf else "—")
        # La precision seule révèle un problème que le recall seul masque :
        # un modèle qui bloque presque tout aurait un excellent recall tout
        # en générant énormément de faux positifs — cas réel observé
        # (recall 0.98, precision tombée à 0.29 en trafic continu).
        col4.metric("Precision live", f"{perf['precision']:.2f}" if perf else "—")
        col5.metric("Version modèle", latest.get("model_version", "—"))

        if latest.get("alert_reasons"):
            st.warning("Alerte déclenchée : " + " ; ".join(latest["alert_reasons"]))
        if latest.get("retrain_triggered"):
            st.success("Réentraînement automatique déclenché à ce cycle.")

        checked_at = latest.get("checked_at")
        ts_str = checked_at.strftime("%Y-%m-%d %H:%M:%S UTC") if hasattr(checked_at, "strftime") else str(checked_at)
        st.caption(f"Dernier check : {ts_str} · référence={latest.get('n_reference', '?')} lignes"
                   f" · fenêtre courante={latest.get('n_current', '?')} lignes")

    st.divider()

    st.subheader("Évolution drift / recall / precision dans le temps")
    history = ml_monitoring_history(50)
    if history:
        rows = []
        for h in reversed(history):  # remet en ordre chronologique pour le graphe
            perf_h = h.get("performance") or {}
            drift_h = h.get("drift") or {}
            rows.append({
                "checked_at": h.get("checked_at"),
                "drift_share": drift_h.get("drift_share"),
                "recall": perf_h.get("recall"),
                "precision": perf_h.get("precision"),
                "retrain": bool(h.get("retrain_triggered")),
            })
        df_hist = pd.DataFrame(rows)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_hist["checked_at"], y=df_hist["drift_share"],
            name="Drift share", line=dict(color="#ff8800", width=2)
        ))
        fig.add_trace(go.Scatter(
            x=df_hist["checked_at"], y=df_hist["recall"],
            name="Recall live", line=dict(color="#635BFF", width=2)
        ))
        fig.add_trace(go.Scatter(
            x=df_hist["checked_at"], y=df_hist["precision"],
            name="Precision live", line=dict(color="#ff4444", width=2, dash="dot")
        ))
        retrains = df_hist[df_hist["retrain"]]
        if not retrains.empty:
            # Marqueurs distincts pour repérer visuellement quand un
            # réentraînement automatique a été déclenché sur la chronologie.
            fig.add_trace(go.Scatter(
                x=retrains["checked_at"], y=[1.02] * len(retrains), mode="markers",
                name="Réentraînement déclenché",
                marker=dict(color="#ff4444", size=10, symbol="triangle-down")
            ))
        fig.update_layout(
            height=300, margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=1.15),
            xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333", range=[0, 1.1]),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Pas encore d'historique de monitoring à afficher")

    st.divider()

    st.subheader("Distribution des scores par version de modèle")
    df_scores = fraud_score_by_model_version()
    if not df_scores.empty:
        fig_hist = px.histogram(
            df_scores, x="fraud_score", color="model_version", nbins=20,
            color_discrete_sequence=["#635BFF", "#ff8800"], barmode="overlay", opacity=0.7,
        )
        fig_hist.update_layout(
            height=280, margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=1.1),
            xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    else:
        st.info("Pas encore d'alertes scorées pour comparer rule-based-v1 et xgboost-v1")

    st.divider()

    mlflow_ui = os.environ.get("MLFLOW_UI_URL", "http://localhost:5001")
    st.markdown(f"[Ouvrir MLflow — runs d'entraînement et registre de modèles]({mlflow_ui})")
    st.caption("Chaque réentraînement (manuel via `make ml-train`, ou automatique via ml-monitor) "
               "crée un nouveau run tracé : hyperparamètres, métriques, modèle versionné.")

# ── FOOTER + AUTO-REFRESH ──────────────────────────────────────────────────────
st.caption("Certification AIA RNCP41993 · Bloc 2 · Patrice Duclos · 2026")

# Boucle de rafraîchissement pilotée par le slider sidebar.
# Le sleep bloque le script courant, puis rerun relance tout le render cycle.
time.sleep(refresh)
st.rerun()
