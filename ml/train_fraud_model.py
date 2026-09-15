#!/usr/bin/env python3
"""Entraîne le modèle de scoring fraude (XGBoost) sur l'historique PostgreSQL.

Le label d'entraînement est `metadata->>'is_fraud_pattern'`, le flag de
vérité terrain posé par le générateur de démo (producers/transaction_producer.py)
au moment de la création de chaque transaction — pas `fraud_indicators.decision`,
qui est la sortie du moteur à règles actuel : entraîner sur les décisions du
moteur à règles ne ferait que réapprendre ses propres seuils, pas détecter
la fraude simulée réellement injectée par le générateur.

Usage : make ml-train  (ou ./venv/bin/python ml/train_fraud_model.py)
Sortie : ml/models/fraud_xgboost_v1.pkl + ml/models/fraud_xgboost_v1.meta.json
"""
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sklearn.metrics import (
    classification_report, roc_auc_score, precision_score, recall_score, f1_score
)
import xgboost as xgb
import joblib

from ml.features import FEATURE_NAMES, build_feature_vector

PG_CONFIG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", 5432)),
    dbname=os.environ.get("PG_DB", "stripe_oltp"),
    user=os.environ.get("PG_USER", "stripe_app"),
    password=os.environ.get("PG_PASSWORD", ""),
)

MODEL_VERSION = "xgboost-v1"
MODEL_DIR = Path(__file__).resolve().parent / "models"
MODEL_PATH = MODEL_DIR / f"fraud_{MODEL_VERSION}.pkl"
META_PATH = MODEL_DIR / f"fraud_{MODEL_VERSION}.meta.json"

# Minimum de lignes pour qu'un split train/test temporel ait un sens
# statistique — en dessous, le modèle entraîné serait trop instable pour
# être utile (mieux vaut lancer le producer plus longtemps avant d'entraîner).
MIN_ROWS = 200


def extract_training_data():
    """Extrait les transactions avec vélocité recalculée et label de vérité terrain.

    La vélocité est recalculée via une sous-requête corrélée (fenêtre glissante
    par client, bornée AVANT chaque transaction) pour reproduire exactement ce
    que Redis expose en temps réel au moment du scoring live — condition
    nécessaire pour que le modèle entraîné généralise au contexte d'inférence.

    Returns:
        pd.DataFrame: une ligne par transaction, avec colonnes brutes + label.
    """
    conn = psycopg2.connect(**PG_CONFIG)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                t.txn_id,
                t.amount,
                t.created_at,
                t.ip_country,
                t.device_type,
                -- Vélocité recalculée : transactions du MÊME client, STRICTEMENT
                -- avant l'horodatage courant, sur les fenêtres 1h/24h — même
                -- sémantique que les sorted sets Redis v1h_<id>/v24h_<id>.
                (SELECT COUNT(*) FROM transactions t2
                   WHERE t2.customer_id = t.customer_id
                     AND t2.created_at >= t.created_at - INTERVAL '1 hour'
                     AND t2.created_at < t.created_at) AS velocity_1h,
                (SELECT COUNT(*) FROM transactions t2
                   WHERE t2.customer_id = t.customer_id
                     AND t2.created_at >= t.created_at - INTERVAL '24 hours'
                     AND t2.created_at < t.created_at) AS velocity_24h,
                -- Vérité terrain posée par le générateur, pas la décision du
                -- moteur à règles (cf. docstring module).
                (t.metadata->>'is_fraud_pattern')::boolean AS is_fraud
            FROM transactions t
            WHERE t.customer_id IS NOT NULL
              AND t.metadata ? 'is_fraud_pattern'
            ORDER BY t.created_at
        """)
        rows = cur.fetchall()
    conn.close()
    return pd.DataFrame(rows)


def build_dataset(df: pd.DataFrame):
    """Transforme le DataFrame brut en matrice de features (X) et labels (y).

    Args:
        df (pd.DataFrame): Sortie de extract_training_data().

    Returns:
        tuple: (X: np.ndarray [n, len(FEATURE_NAMES)], y: np.ndarray [n]).
    """
    vectors = [
        build_feature_vector(
            amount=row["amount"],
            created_at=row["created_at"],
            ip_country=row["ip_country"],
            device_type=row["device_type"],
            velocity_1h=row["velocity_1h"],
            velocity_24h=row["velocity_24h"],
        )
        for _, row in df.iterrows()
    ]
    X = np.vstack(vectors)
    y = df["is_fraud"].astype(int).to_numpy()
    return X, y


def temporal_train_test_split(df: pd.DataFrame, X: np.ndarray, y: np.ndarray, test_frac=0.2):
    """Split train/test par ordre chronologique (pas aléatoire).

    Un split aléatoire mélangerait les fenêtres de vélocité passé/futur de
    façon irréaliste (le modèle "verrait" indirectement des transactions
    futures via la corrélation temporelle des clients) — le split temporel
    évite cette fuite d'information (leakage) et simule la vraie situation
    de production : entraîner sur le passé, valider sur le futur.
    """
    # df est déjà trié par created_at (ORDER BY dans la requête SQL).
    n = len(df)
    cut = int(n * (1 - test_frac))
    return X[:cut], X[cut:], y[:cut], y[cut:]


def train_model(X_train, y_train):
    """Entraîne un classifieur XGBoost avec pondération de classe.

    Args:
        X_train, y_train: Données d'entraînement.

    Returns:
        xgb.XGBClassifier: Modèle entraîné.
    """
    n_pos = int(y_train.sum())
    n_neg = len(y_train) - n_pos
    # scale_pos_weight compense le déséquilibre de classes (peu de fraude vs
    # beaucoup de légitime, cf. FRAUD_RATIO ~5% dans transaction_producer.py) —
    # sans ce poids, le modèle apprendrait trivialement à toujours prédire "légitime".
    scale_pos_weight = (n_neg / n_pos) if n_pos > 0 else 1.0

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.1,
        scale_pos_weight=scale_pos_weight,
        eval_metric="logloss",
        random_state=42,
    )
    model.fit(X_train, y_train)
    return model


def evaluate_model(model, X_test, y_test):
    """Calcule les métriques de performance sur le jeu de test temporel.

    Returns:
        dict: precision, recall, f1, roc_auc, classification_report (str).
    """
    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= 0.5).astype(int)

    metrics = {
        "precision": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall": float(recall_score(y_test, y_pred, zero_division=0)),
        "f1": float(f1_score(y_test, y_pred, zero_division=0)),
        # roc_auc nécessite les deux classes présentes dans y_test — protégé
        # pour ne pas planter sur un petit échantillon de test déséquilibré.
        "roc_auc": float(roc_auc_score(y_test, y_proba)) if len(set(y_test)) > 1 else None,
        "n_test": len(y_test),
        "n_test_fraud": int(y_test.sum()),
    }
    report = classification_report(y_test, y_pred, target_names=["legit", "fraud"], zero_division=0)
    return metrics, report


def main():
    print(f"[START] Entraînement modèle fraude ({MODEL_VERSION})")
    print(f"   Source : {PG_CONFIG['host']}/{PG_CONFIG['dbname']}")

    print("Extraction des données d'entraînement...")
    df = extract_training_data()
    print(f"  → {len(df)} transactions extraites")

    if len(df) < MIN_ROWS:
        print(f"[ERROR] Pas assez de données ({len(df)} < {MIN_ROWS} requis).")
        print("   Lance le producer plus longtemps : make producer")
        sys.exit(1)

    fraud_count = int(df["is_fraud"].sum())
    print(f"   dont {fraud_count} fraudes ({fraud_count/len(df)*100:.1f}%)")

    print("\nConstruction des features...")
    X, y = build_dataset(df)
    print(f"  → matrice {X.shape}, features : {FEATURE_NAMES}")

    print("\nSplit temporel train/test (80/20)...")
    X_train, X_test, y_train, y_test = temporal_train_test_split(df, X, y)
    print(f"  → train : {len(X_train)} lignes ({int(y_train.sum())} fraudes)")
    print(f"  → test  : {len(X_test)} lignes ({int(y_test.sum())} fraudes)")

    print("\nEntraînement XGBoost...")
    model = train_model(X_train, y_train)
    print("  [OK] Modèle entraîné")

    print("\nÉvaluation sur le jeu de test...")
    metrics, report = evaluate_model(model, X_test, y_test)
    print(report)
    print(f"  ROC AUC : {metrics['roc_auc']}")

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    meta = {
        "model_version": MODEL_VERSION,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "feature_names": FEATURE_NAMES,
        "n_train": len(X_train),
        "n_test": len(X_test),
        "metrics": metrics,
    }
    META_PATH.write_text(json.dumps(meta, indent=2))

    print(f"\n[OK] Modèle sauvegardé : {MODEL_PATH}")
    print(f"[OK] Métadonnées sauvegardées : {META_PATH}")
    print(f"\nPour l'activer dans le scorer : export SCORING_ENGINE=ml")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Erreur entraînement : {e}", file=sys.stderr)
        sys.exit(1)
