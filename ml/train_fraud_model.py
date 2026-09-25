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
    average_precision_score,
    classification_report, roc_auc_score, precision_score, recall_score, f1_score
)
import xgboost as xgb
import joblib
import mlflow
import mlflow.xgboost

from ml.features import FEATURE_NAMES, build_feature_vector

# Si absent (usage hors Docker, ex. lancé depuis le Mac host), retombe sur un
# tracking local fichier plutôt que de planter — cohérent avec le principe de
# fallback déjà appliqué à SCORING_ENGINE dans producers/flink_like_job.py.
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "file:./mlruns")
MLFLOW_EXPERIMENT = "fraud-detection"

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
MIN_TREES = 100  # nombre d'arbres minimal accepté après arrêt anticipé (cf. train_model)


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


def extract_current_window(minutes: int):
    """Extrait les transactions des `minutes` dernières minutes — utilisé par
    ml/monitor.py comme fenêtre "courante" à comparer à la référence d'entraînement.

    Mêmes colonnes et même calcul de vélocité que extract_training_data() (la
    comparaison drift/performance n'a de sens que si les deux jeux de données
    sont construits de façon strictement identique).

    Args:
        minutes (int): Taille de la fenêtre glissante en minutes.

    Returns:
        pd.DataFrame: une ligne par transaction récente.
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
                (SELECT COUNT(*) FROM transactions t2
                   WHERE t2.customer_id = t.customer_id
                     AND t2.created_at >= t.created_at - INTERVAL '1 hour'
                     AND t2.created_at < t.created_at) AS velocity_1h,
                (SELECT COUNT(*) FROM transactions t2
                   WHERE t2.customer_id = t.customer_id
                     AND t2.created_at >= t.created_at - INTERVAL '24 hours'
                     AND t2.created_at < t.created_at) AS velocity_24h,
                (t.metadata->>'is_fraud_pattern')::boolean AS is_fraud
            FROM transactions t
            WHERE t.customer_id IS NOT NULL
              AND t.metadata ? 'is_fraud_pattern'
              -- make_interval() plutôt qu'un INTERVAL littéral : permet de
              -- passer `minutes` comme paramètre lié au lieu de l'interpoler
              -- dans la chaîne SQL.
              AND t.created_at >= NOW() - make_interval(mins => %s)
            ORDER BY t.created_at
        """, (minutes,))
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
        tuple: (xgb.XGBClassifier entraîné, dict des hyperparamètres utilisés
        — renvoyés séparément pour que main() puisse les logger dans MLflow
        sans dupliquer leur définition).
    """
    n_pos = int(y_train.sum())
    n_neg = len(y_train) - n_pos
    # scale_pos_weight compense le déséquilibre de classes (peu de fraude vs
    # beaucoup de légitime, cf. FRAUD_RATIO ~5% dans transaction_producer.py) —
    # sans ce poids, le modèle apprendrait trivialement à toujours prédire "légitime".
    scale_pos_weight = (n_neg / n_pos) if n_pos > 0 else 1.0

    # Arrêt anticipé sur une validation TEMPORELLE (les 15 % les plus récents du
    # train) : le nombre d'arbres n'est plus fixé à la main, et le modèle cesse
    # d'apprendre dès que la validation ne progresse plus — protection contre le
    # surapprentissage. Arbres un peu plus profonds mais régularisés
    # (sous-échantillonnage, min_child_weight, L2) ; métrique aucpr, plus
    # informative que logloss sur une classe positive minoritaire.
    n_val = max(int(len(X_train) * 0.15), 1)
    X_fit, y_fit = X_train[:-n_val], y_train[:-n_val]
    X_val, y_val = X_train[-n_val:], y_train[-n_val:]

    params = dict(
        n_estimators=600,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_lambda=2.0,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",
        early_stopping_rounds=30,
        random_state=42,
    )
    model = xgb.XGBClassifier(**params)
    model.fit(X_fit, y_fit, eval_set=[(X_val, y_val)], verbose=False)
    params["best_iteration"] = int(model.best_iteration)
    # Garde-fou : si la validation plafonne tout de suite (aucpr plat sur une
    # fenêtre bruitée), l'arrêt anticipé garde quelques arbres à peine et les
    # probabilités restent trop tièdes pour franchir le seuil de blocage (0,85)
    # — observé le 25/09 (recall_at_block = 0). On impose alors MIN_TREES arbres.
    if params["best_iteration"] + 1 < MIN_TREES:
        print(f"  Arrêt anticipé trop précoce ({params['best_iteration'] + 1} arbres) : réentraînement à {MIN_TREES} arbres fixes")
        fixed = {k: v for k, v in params.items() if k not in ("early_stopping_rounds", "best_iteration")}
        fixed["n_estimators"] = MIN_TREES
        model = xgb.XGBClassifier(**fixed)
        model.fit(X_train, y_train, verbose=False)
        params["best_iteration"] = MIN_TREES - 1
        params["forced_min_trees"] = True
    print(f"  Arrêt anticipé : {params['best_iteration'] + 1} arbres retenus sur {params['n_estimators']}")
    return model, params


def evaluate_model(model, X_test, y_test):
    """Calcule les métriques de performance sur le jeu de test temporel.

    Returns:
        dict: precision, recall, f1, roc_auc, classification_report (str).
    """
    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= 0.5).astype(int)

    # Seuil de blocage réellement servi (cf. FRAUD_SCORE_THRESHOLD du scorer) :
    # la précision/rappel « à 0.5 » est la métrique académique, celle « au
    # blocage » est ce que le monitoring mesure ensuite en production.
    block_thr = float(os.environ.get("FRAUD_SCORE_THRESHOLD", 0.85))
    y_block = (y_proba >= block_thr).astype(int)

    metrics = {
        "precision": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall": float(recall_score(y_test, y_pred, zero_division=0)),
        "f1": float(f1_score(y_test, y_pred, zero_division=0)),
        "precision_at_block": float(precision_score(y_test, y_block, zero_division=0)),
        "recall_at_block": float(recall_score(y_test, y_block, zero_division=0)),
        "average_precision": float(average_precision_score(y_test, y_proba)) if len(set(y_test)) > 1 else None,
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
    print(f"   MLflow : {MLFLOW_TRACKING_URI}")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    print("Extraction des données d'entraînement...")
    df = extract_training_data()
    print(f"  → {len(df)} transactions extraites")

    if len(df) < MIN_ROWS:
        print(f"[ERROR] Pas assez de données ({len(df)} < {MIN_ROWS} requis).")
        print("   Lance le producer plus longtemps : make producer")
        sys.exit(1)

    fraud_count = int(df["is_fraud"].sum())
    fraud_ratio = fraud_count / len(df)
    print(f"   dont {fraud_count} fraudes ({fraud_ratio*100:.1f}%)")

    print("\nConstruction des features...")
    X, y = build_dataset(df)
    print(f"  → matrice {X.shape}, features : {FEATURE_NAMES}")

    print("\nSplit temporel train/test (80/20)...")
    X_train, X_test, y_train, y_test = temporal_train_test_split(df, X, y)
    print(f"  → train : {len(X_train)} lignes ({int(y_train.sum())} fraudes)")
    print(f"  → test  : {len(X_test)} lignes ({int(y_test.sum())} fraudes)")

    # Un run MLflow par entraînement : params, métriques et modèle sérialisé
    # sont tracés ensemble, consultables dans l'UI MLflow (http://localhost:5000)
    # pour comparer les versions successives du modèle au fil des réentraînements.
    with mlflow.start_run(run_name=f"{MODEL_VERSION}-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"):
        print("\nEntraînement XGBoost...")
        model, params = train_model(X_train, y_train)
        print("  [OK] Modèle entraîné")
        mlflow.log_params(params)
        mlflow.log_param("n_train", len(X_train))
        mlflow.log_param("n_test", len(X_test))
        mlflow.log_param("fraud_ratio_train", fraud_ratio)
        mlflow.log_param("feature_names", FEATURE_NAMES)

        print("\nÉvaluation sur le jeu de test...")
        metrics, report = evaluate_model(model, X_test, y_test)
        print(report)
        print(f"  ROC AUC : {metrics['roc_auc']}")
        # None n'est pas loggable comme métrique MLflow (roc_auc peut être None
        # si y_test n'a qu'une seule classe, cf. evaluate_model) — filtré ici.
        mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, (int, float))})

        # Enregistre le modèle dans le Model Registry MLflow : chaque run crée
        # une nouvelle version sous le même nom, l'historique complet reste
        # consultable (contrairement au .pkl local qui écrase la version précédente).
        mlflow.xgboost.log_model(model, artifact_path="model", registered_model_name="fraud-detector")

        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, MODEL_PATH)
        meta = {
            "model_version": MODEL_VERSION,
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "feature_names": FEATURE_NAMES,
            "n_train": len(X_train),
            "n_test": len(X_test),
            "metrics": metrics,
            "mlflow_run_id": mlflow.active_run().info.run_id,
        }
        META_PATH.write_text(json.dumps(meta, indent=2))

    print(f"\n[OK] Modèle sauvegardé : {MODEL_PATH}")
    print(f"[OK] Métadonnées sauvegardées : {META_PATH}")
    print(f"[OK] Run MLflow tracé : {MLFLOW_TRACKING_URI}")
    print(f"\nPour l'activer dans le scorer : export SCORING_ENGINE=ml")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Erreur entraînement : {e}", file=sys.stderr)
        sys.exit(1)
