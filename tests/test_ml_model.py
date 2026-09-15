#!/usr/bin/env python3
"""Tests du module ML (ml/) — n'ont pas besoin de la stack Docker.

Contrairement à tests/test_e2e.py (qui valide le pipeline live), ces tests
valident la logique pure : feature engineering, cycle entraînement/sauvegarde/
chargement d'un modèle synthétique, et le comportement de fallback du scorer
quand aucun modèle n'est encore entraîné. Utile pour vérifier le code ML sans
dépendre de Postgres/Kafka/Redis.

Usage : ./venv/bin/python tests/test_ml_model.py
"""
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _env  # noqa: F401

import numpy as np

PASS, FAIL = "[OK]", "[FAIL]"
results = []


def check(name, fn):
    try:
        fn()
        results.append((PASS, name))
        print(f"{PASS} {name}")
    except AssertionError as e:
        results.append((FAIL, name, str(e)))
        print(f"{FAIL} {name}  →  {e}")
    except Exception as e:
        results.append((FAIL, name, str(e)))
        print(f"{FAIL} {name}  →  {type(e).__name__}: {e}")


# ─── Feature engineering ─────────────────────────────────────────────────────

def test_feature_vector_shape_and_order():
    """build_feature_vector renvoie un vecteur de la bonne longueur, dans l'ordre FEATURE_NAMES."""
    from ml.features import build_feature_vector, FEATURE_NAMES
    vec = build_feature_vector(
        amount=150000, created_at=datetime(2026, 3, 15, 14, 30, tzinfo=timezone.utc),
        ip_country="RU", device_type="pos", velocity_1h=12, velocity_24h=60,
    )
    assert vec.shape == (len(FEATURE_NAMES),), f"shape={vec.shape}, attendu ({len(FEATURE_NAMES)},)"
    assert vec.dtype == np.float32, f"dtype={vec.dtype}"


def test_feature_vector_high_risk_signals():
    """Les signaux de risque (pays, device, vélocité) se reflètent correctement dans le vecteur."""
    from ml.features import build_feature_vector, FEATURE_NAMES
    high_risk = build_feature_vector(
        amount=150000, created_at=datetime.now(timezone.utc),
        ip_country="RU", device_type="pos", velocity_1h=20, velocity_24h=100,
    )
    low_risk = build_feature_vector(
        amount=5000, created_at=datetime.now(timezone.utc),
        ip_country="FR", device_type="desktop", velocity_1h=1, velocity_24h=3,
    )
    idx = {name: i for i, name in enumerate(FEATURE_NAMES)}
    assert high_risk[idx["is_high_risk_country"]] == 1.0
    assert low_risk[idx["is_high_risk_country"]] == 0.0
    assert high_risk[idx["is_pos_device"]] == 1.0
    assert low_risk[idx["is_pos_device"]] == 0.0
    assert high_risk[idx["velocity_1h"]] > low_risk[idx["velocity_1h"]]
    # Le montant élevé doit produire un amount_log plus grand (log1p croissant).
    assert high_risk[idx["amount_log"]] > low_risk[idx["amount_log"]]


def test_feature_vector_handles_missing_country():
    """ip_country=None/'' ne doit pas planter, et n'est jamais considéré à risque."""
    from ml.features import build_feature_vector, FEATURE_NAMES
    vec = build_feature_vector(
        amount=1000, created_at=None, ip_country=None, device_type=None,
        velocity_1h=None, velocity_24h=None,
    )
    idx = {name: i for i, name in enumerate(FEATURE_NAMES)}
    assert vec[idx["is_high_risk_country"]] == 0.0
    assert vec[idx["velocity_1h"]] == 0.0


# ─── Cycle entraînement / sauvegarde / chargement (données synthétiques) ────

def _make_synthetic_training_set(n=400, seed=0):
    """Génère un jeu de données synthétique où la fraude est clairement séparable
    (montant élevé + pays à risque) — sert uniquement à valider le pipeline
    ml/, pas à évaluer la performance réelle du modèle (cf. ml/train_fraud_model.py
    pour l'entraînement sur données réelles)."""
    from ml.features import build_feature_vector
    rng = np.random.default_rng(seed)
    X, y = [], []
    for _ in range(n):
        is_fraud = rng.random() < 0.15
        if is_fraud:
            amount = rng.integers(100_000, 300_000)
            country = rng.choice(["RU", "NG", "KP"])
            velocity_1h = rng.integers(10, 30)
        else:
            amount = rng.integers(500, 50_000)
            country = rng.choice(["FR", "US", "DE"])
            velocity_1h = rng.integers(0, 5)
        vec = build_feature_vector(
            amount=int(amount), created_at=datetime.now(timezone.utc),
            ip_country=country, device_type="desktop",
            velocity_1h=int(velocity_1h), velocity_24h=int(velocity_1h) * 3,
        )
        X.append(vec)
        y.append(int(is_fraud))
    return np.vstack(X), np.array(y)


def test_train_predict_save_load_roundtrip():
    """Entraîne un modèle sur données synthétiques séparables, vérifie qu'il
    discrimine correctement, puis valide le cycle sauvegarde/rechargement."""
    import xgboost as xgb
    import joblib

    X, y = _make_synthetic_training_set()
    n_pos, n_neg = int(y.sum()), int((1 - y).sum())
    model = xgb.XGBClassifier(
        n_estimators=50, max_depth=3, learning_rate=0.2,
        scale_pos_weight=n_neg / n_pos, eval_metric="logloss", random_state=42,
    )
    model.fit(X, y)

    # Sur un jeu synthétique clairement séparable, le modèle doit apprendre
    # le pattern quasi-parfaitement (pas un vrai seuil de perf, juste une
    # preuve que le pipeline fonctionne de bout en bout).
    preds = (model.predict_proba(X)[:, 1] >= 0.5).astype(int)
    accuracy = (preds == y).mean()
    assert accuracy > 0.90, f"accuracy trop basse sur données synthétiques séparables : {accuracy:.2f}"

    tmpdir = Path(tempfile.mkdtemp())
    try:
        model_path = tmpdir / "test_model.pkl"
        joblib.dump(model, model_path)
        reloaded = joblib.load(model_path)
        reloaded_preds = (reloaded.predict_proba(X)[:, 1] >= 0.5).astype(int)
        assert np.array_equal(preds, reloaded_preds), "le modèle rechargé donne des prédictions différentes"
    finally:
        shutil.rmtree(tmpdir)


# ─── Comportement de ml/scoring.py (fallback si modèle absent) ─────────────

def test_scoring_returns_none_without_model():
    """score() doit renvoyer None (pas lever d'exception) si le modèle n'a pas encore été entraîné."""
    import ml.scoring as scoring_module

    # Pointe temporairement vers un chemin inexistant pour simuler l'état
    # "pas encore entraîné", sans dépendre du fait qu'un vrai modèle existe
    # ou non sur cette machine.
    original_path = scoring_module.MODEL_PATH
    original_cache = scoring_module._model_cache
    original_attempted = scoring_module._model_load_attempted
    try:
        scoring_module.MODEL_PATH = Path(tempfile.mkdtemp()) / "does_not_exist.pkl"
        scoring_module._model_cache = None
        scoring_module._model_load_attempted = False

        result = scoring_module.score(
            amount=150000, created_at=datetime.now(timezone.utc),
            ip_country="RU", device_type="pos", velocity_1h=20, velocity_24h=80,
        )
        assert result is None, f"attendu None (pas de modèle), reçu {result}"
    finally:
        scoring_module.MODEL_PATH = original_path
        scoring_module._model_cache = original_cache
        scoring_module._model_load_attempted = original_attempted


def test_score_transaction_falls_back_to_rules_when_ml_unavailable():
    """score_transaction() avec SCORING_ENGINE=ml et aucun modèle entraîné
    doit produire le même résultat que le moteur à règles (fallback), pas planter."""
    import producers.flink_like_job as job
    import ml.scoring as scoring_module

    original_engine = job.SCORING_ENGINE
    original_path = scoring_module.MODEL_PATH
    original_cache = scoring_module._model_cache
    original_attempted = scoring_module._model_load_attempted
    try:
        job.SCORING_ENGINE = "ml"
        scoring_module.MODEL_PATH = Path(tempfile.mkdtemp()) / "does_not_exist.pkl"
        scoring_module._model_cache = None
        scoring_module._model_load_attempted = False

        class _FakeRedis:
            """Simule l'API Redis minimale utilisée par score_transaction, sans instance réelle."""
            def zadd(self, *a, **k): pass
            def zremrangebyscore(self, *a, **k): pass
            def expire(self, *a, **k): pass
            def zcard(self, *a, **k): return 0
            def hset(self, *a, **k): pass

        txn = {
            "customer_id": "test-customer", "amount": 150000,
            "ip_country": "RU", "device_type": "pos",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        result = job.score_transaction(txn, _FakeRedis())
        assert result is not None
        assert result["model_version"] == "rule-based-v1", (
            f"attendu fallback rule-based-v1, reçu {result['model_version']}"
        )
        assert "R1_high_amount" in result["rules_triggered"], "règle montant élevé non déclenchée"
        assert "R3_high_risk_geo" in result["rules_triggered"], "règle géo à risque non déclenchée"
    finally:
        job.SCORING_ENGINE = original_engine
        scoring_module.MODEL_PATH = original_path
        scoring_module._model_cache = original_cache
        scoring_module._model_load_attempted = original_attempted


# ─── Runner ─────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Tests ML — feature engineering + cycle modèle (sans Docker)")
    print("=" * 60)

    print("\n── Feature engineering ──")
    check("Vecteur de features : forme et ordre", test_feature_vector_shape_and_order)
    check("Vecteur de features : signaux de risque cohérents", test_feature_vector_high_risk_signals)
    check("Vecteur de features : gère les champs manquants", test_feature_vector_handles_missing_country)

    print("\n── Cycle entraînement/sauvegarde/chargement ──")
    check("Train → predict → save → reload (données synthétiques)", test_train_predict_save_load_roundtrip)

    print("\n── Fallback rule-based si modèle absent ──")
    check("scoring.score() renvoie None sans modèle entraîné", test_scoring_returns_none_without_model)
    check("score_transaction() retombe sur les règles si SCORING_ENGINE=ml sans modèle", test_score_transaction_falls_back_to_rules_when_ml_unavailable)

    passed = sum(1 for r in results if r[0] == PASS)
    failed = sum(1 for r in results if r[0] == FAIL)
    print("\n" + "=" * 60)
    print(f"  Résultats : {passed}/{len(results)} tests passés")
    if failed:
        print(f"  {FAIL} {failed} échec(s) :")
        for r in results:
            if r[0] == FAIL:
                print(f"     - {r[1]} → {r[2] if len(r) > 2 else ''}")
    print("=" * 60)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
