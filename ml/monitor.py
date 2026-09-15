#!/usr/bin/env python3
"""Monitoring continu du modèle de fraude : drift (Evidently) + performance
live, avec déclenchement automatique d'un réentraînement en cas de dérive.

Boucle infinie : toutes les ML_MONITOR_INTERVAL_SECONDS, compare la fenêtre
de référence (données d'entraînement) à la fenêtre courante (transactions
récentes) sur deux axes :
- Data drift (Evidently) : les features en entrée ont-elles changé de
  distribution par rapport à ce que le modèle a vu à l'entraînement ?
- Performance : le modèle actuellement en production détecte-t-il encore
  correctement la fraude sur les données fraîches (recall) ?

Si l'un des deux seuils est franchi, relance ml/train_fraud_model.py — le
nouveau modèle écrase ml/models/fraud_xgboost-v1.pkl et devient actif au
prochain chargement (cf. limite documentée en fin de fichier).

Usage : python -m ml.monitor  (tourne dans le service Docker ml-monitor)
"""
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

import pandas as pd
import mlflow
from pymongo import MongoClient
from sklearn.metrics import precision_score, recall_score, f1_score
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

from ml.features import FEATURE_NAMES
from ml.train_fraud_model import (
    extract_training_data, temporal_train_test_split, build_dataset,
    extract_current_window, main as retrain_model,
    MODEL_PATH as TRAINED_MODEL_PATH, MODEL_VERSION, MIN_ROWS,
)

MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_USER = os.environ.get("MONGO_USER", "admin")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD", "")
MONGO_DB = os.environ.get("MONGO_DB", "stripe_nosql")

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "file:./mlruns")

INTERVAL_SECONDS = int(os.environ.get("ML_MONITOR_INTERVAL_SECONDS", 120))
WINDOW_MINUTES = int(os.environ.get("ML_MONITOR_WINDOW_MINUTES", 30))
DRIFT_THRESHOLD = float(os.environ.get("ML_DRIFT_THRESHOLD", 0.3))
MIN_RECALL = float(os.environ.get("ML_MIN_RECALL", 0.7))
# Évite de relancer un entraînement à chaque cycle tant que le problème
# détecté persiste (ex. drift qui dure plusieurs cycles) — laisse le temps
# à un run précédent de se refléter avant d'en redéclencher un autre.
RETRAIN_COOLDOWN_SECONDS = INTERVAL_SECONDS * 3
# En dessous de ce volume sur la fenêtre courante, le calcul de drift/recall
# serait trop bruité pour être exploitable (quelques transactions ne
# représentent pas une vraie distribution).
MIN_CURRENT_ROWS = 30


def get_mongo_db():
    if MONGO_USER and MONGO_PASSWORD:
        uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    else:
        uri = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"
    return MongoClient(uri, serverSelectionTimeoutMS=5000)[MONGO_DB]


def compute_drift(X_reference, X_current):
    """Calcule le score de drift Evidently entre les features de référence et courantes.

    Args:
        X_reference, X_current (np.ndarray): Matrices de features (même colonnes, FEATURE_NAMES).

    Returns:
        dict: {"drift_share": float, "dataset_drift": bool} — drift_share est
        la proportion de colonnes dont la distribution a significativement changé.
    """
    ref_df = pd.DataFrame(X_reference, columns=FEATURE_NAMES)
    cur_df = pd.DataFrame(X_current, columns=FEATURE_NAMES)
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=ref_df, current_data=cur_df)
    result = report.as_dict()["metrics"][0]["result"]
    return {
        "drift_share": float(result["share_of_drifted_columns"]),
        "dataset_drift": bool(result["dataset_drift"]),
    }


def compute_live_performance(model, X_current, y_current):
    """Applique le modèle actuel aux données courantes et calcule precision/recall/f1.

    Args:
        model: Modèle XGBoost chargé depuis disque.
        X_current, y_current: Features et labels (vérité terrain) de la fenêtre courante.

    Returns:
        dict: precision, recall, f1 — mesure la capacité du modèle EN PRODUCTION
        (pas au moment de son entraînement) à détecter la fraude sur des
        données fraîches, ce qui capture la dégradation dans le temps
        (concept drift), pas seulement le drift des features en entrée.
    """
    y_pred = (model.predict_proba(X_current)[:, 1] >= 0.5).astype(int)
    return {
        "precision": float(precision_score(y_current, y_pred, zero_division=0)),
        "recall": float(recall_score(y_current, y_pred, zero_division=0)),
        "f1": float(f1_score(y_current, y_pred, zero_division=0)),
    }


def check_once(db, last_retrain_at):
    """Exécute un cycle de monitoring complet. Retourne le nouveau last_retrain_at."""
    checked_at = datetime.now(timezone.utc)
    doc = {"checked_at": checked_at, "model_version": MODEL_VERSION}

    ref_df = extract_training_data()
    if len(ref_df) < MIN_ROWS:
        doc["status"] = "skipped"
        doc["reason"] = f"pas assez de données de référence ({len(ref_df)} < {MIN_ROWS})"
        db.ml_monitoring.insert_one(doc)
        print(f"[SKIP] {doc['reason']}")
        return last_retrain_at

    X_ref, y_ref = build_dataset(ref_df)
    # Référence = exactement le split d'entraînement utilisé par le dernier
    # run (même fonction, même paramètre test_frac) — comparer contre le test
    # set introduirait un biais (ces données n'ont jamais servi à entraîner).
    X_ref_train, _, _, _ = temporal_train_test_split(ref_df, X_ref, y_ref)

    cur_df = extract_current_window(WINDOW_MINUTES)
    if len(cur_df) < MIN_CURRENT_ROWS:
        doc["status"] = "skipped"
        doc["reason"] = f"pas assez de transactions récentes ({len(cur_df)} < {MIN_CURRENT_ROWS} sur {WINDOW_MINUTES}min)"
        db.ml_monitoring.insert_one(doc)
        print(f"[SKIP] {doc['reason']}")
        return last_retrain_at

    X_cur, y_cur = build_dataset(cur_df)

    drift = compute_drift(X_ref_train, X_cur)
    doc["drift"] = drift
    doc["n_reference"] = len(X_ref_train)
    doc["n_current"] = len(X_cur)

    performance = None
    if TRAINED_MODEL_PATH.exists():
        import joblib
        model = joblib.load(TRAINED_MODEL_PATH)
        performance = compute_live_performance(model, X_cur, y_cur)
        doc["performance"] = performance

    reasons = []
    if drift["drift_share"] > DRIFT_THRESHOLD:
        reasons.append(f"drift_share={drift['drift_share']:.2f} > seuil {DRIFT_THRESHOLD}")
    if performance is not None and performance["recall"] < MIN_RECALL:
        reasons.append(f"recall={performance['recall']:.2f} < seuil {MIN_RECALL}")
    if performance is None:
        reasons.append("aucun modèle entraîné pour l'instant")

    doc["status"] = "alert" if reasons else "ok"
    doc["alert_reasons"] = reasons

    now_ts = time.time()
    cooldown_ok = (last_retrain_at is None) or (now_ts - last_retrain_at > RETRAIN_COOLDOWN_SECONDS)
    if reasons and cooldown_ok:
        print(f"[ALERT] Réentraînement déclenché : {'; '.join(reasons)}")
        doc["retrain_triggered"] = True
        try:
            retrain_model()
            last_retrain_at = now_ts
        except SystemExit:
            # extract_training_data() peut sortir via sys.exit(1) si les
            # données sont insuffisantes entre-temps — non fatal pour la boucle.
            print("[WARN] Réentraînement a échoué (données insuffisantes)")
        except Exception as e:
            print(f"[WARN] Réentraînement a échoué : {e}", file=sys.stderr)
    elif reasons:
        doc["retrain_triggered"] = False
        doc["retrain_skipped_reason"] = "cooldown actif"
        print(f"[ALERT] {'; '.join(reasons)} — réentraînement en cooldown, pas relancé")
    else:
        doc["retrain_triggered"] = False
        print(f"[OK] drift={drift['drift_share']:.2f} "
              f"recall={performance['recall']:.2f}" if performance else "[OK] pas de dérive")

    db.ml_monitoring.insert_one(doc)
    return last_retrain_at


def main():
    print(f"[START] ML monitor démarré (intervalle={INTERVAL_SECONDS}s, "
          f"fenêtre={WINDOW_MINUTES}min, seuil drift={DRIFT_THRESHOLD}, seuil recall={MIN_RECALL})")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    db = get_mongo_db()
    last_retrain_at = None

    while True:
        try:
            last_retrain_at = check_once(db, last_retrain_at)
        except Exception as e:
            print(f"[ERROR] Cycle de monitoring en échec : {e}", file=sys.stderr)
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

# NOTE — limite assumée : ml/scoring.py met le modèle en cache mémoire après
# le premier chargement (cf. load_model()) et ne le recharge jamais tant que
# le process scorer tourne. Un réentraînement déclenché ici écrase bien le
# fichier .pkl sur disque, mais le scorer déjà en cours d'exécution continue
# à utiliser l'ancienne version en mémoire jusqu'à son prochain redémarrage.
# Un vrai hot-reload (watcher de fichier, ou polling périodique du mtime du
# .pkl dans load_model()) est listé comme amélioration dans
# docs/ML_INTEGRATION_STRATEGY.md plutôt qu'implémenté ici, pour ne pas
# complexifier le chemin d'inférence chaud avec une I/O disque par appel.
