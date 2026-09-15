"""Inférence du modèle de fraude entraîné (ml/train_fraud_model.py).

Chargement paresseux et mis en cache : le modèle n'est lu depuis disque
qu'une fois par process, pas à chaque transaction scorée — même logique que
les connexions Redis/Postgres réutilisées dans producers/flink_like_job.py.

Si le fichier modèle est absent (pas encore entraîné), score() renvoie None
plutôt que de lever une exception : l'appelant (score_transaction() dans
flink_like_job.py) sait alors retomber sur le moteur à règles — c'est le
mécanisme de fallback décrit dans docs/ML_INTEGRATION_STRATEGY.md §5.3.
"""
from pathlib import Path

from ml.features import build_feature_vector

MODEL_VERSION = "xgboost-v1"
MODEL_PATH = Path(__file__).resolve().parent / "models" / f"fraud_{MODEL_VERSION}.pkl"

_model_cache = None
_model_load_attempted = False


def load_model():
    """Charge le modèle depuis disque (une seule fois, mis en cache en mémoire).

    Returns:
        xgb.XGBClassifier | None: Le modèle chargé, ou None si le fichier
        n'existe pas encore (le modèle n'a pas été entraîné).
    """
    global _model_cache, _model_load_attempted
    if _model_load_attempted:
        return _model_cache
    _model_load_attempted = True

    if not MODEL_PATH.exists():
        return None

    import joblib
    _model_cache = joblib.load(MODEL_PATH)
    return _model_cache


def score(amount, created_at, ip_country, device_type, velocity_1h, velocity_24h):
    """Calcule un fraud_score avec le modèle ML entraîné, si disponible.

    Args:
        amount (int): Montant en centimes.
        created_at (datetime): Horodatage de la transaction.
        ip_country (str): Code pays IP.
        device_type (str): Type d'appareil.
        velocity_1h (int): Vélocité 1h (même source que le moteur à règles — Redis).
        velocity_24h (int): Vélocité 24h.

    Returns:
        float | None: Probabilité de fraude (0.0-1.0), ou None si le modèle
        n'est pas encore entraîné (l'appelant doit alors utiliser le fallback
        rule-based plutôt que de planter).
    """
    model = load_model()
    if model is None:
        return None

    features = build_feature_vector(
        amount=amount, created_at=created_at, ip_country=ip_country,
        device_type=device_type, velocity_1h=velocity_1h, velocity_24h=velocity_24h,
    ).reshape(1, -1)

    # predict_proba retourne [P(classe 0), P(classe 1)] — on ne garde que
    # P(fraude), classe positive dans l'entraînement (cf. train_fraud_model.py).
    proba = model.predict_proba(features)[0, 1]
    return float(proba)
