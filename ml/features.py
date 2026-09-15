"""Feature engineering partagée entre l'entraînement (train_fraud_model.py)
et l'inférence temps réel (scoring.py / producers/flink_like_job.py).

Les mêmes features doivent être calculables des deux côtés :
- à l'entraînement, à partir de l'historique stocké dans Postgres (velocity
  recalculée via fenêtre glissante SQL sur des transactions passées)
- à l'inférence, à partir de l'état Redis live (mêmes définitions de
  fenêtre : transactions du même client sur 1h / 24h glissantes)

Toute divergence entre les deux calculs introduirait un skew training/serving
qui dégraderait silencieusement la précision du modèle en production.
"""
from datetime import datetime, timezone

import numpy as np

# Même liste que producers/flink_like_job.py / flink/fraud_scoring_job.py —
# dupliquée à dessein (cf. convention déjà en place dans ces deux fichiers,
# eux-mêmes dupliqués l'un envers l'autre) plutôt que de forcer un import
# croisé entre le module scoring temps réel et le module ML.
HIGH_RISK_COUNTRIES = {"RU", "NG", "KP", "IR", "VE", "BY"}

# Ordre figé : doit rester identique entre l'entraînement et l'inférence,
# c'est ce qui garantit que la colonne i du vecteur de features veut dire
# la même chose pour le modèle des deux côtés.
FEATURE_NAMES = [
    "amount_log",
    "hour_of_day",
    "day_of_week",
    "is_high_risk_country",
    "is_pos_device",
    "velocity_1h",
    "velocity_24h",
]


def build_feature_vector(amount, created_at, ip_country, device_type, velocity_1h, velocity_24h):
    """Construit un vecteur de features numérique dans l'ordre FEATURE_NAMES.

    Args:
        amount (int): Montant en centimes.
        created_at (datetime): Horodatage de la transaction (timezone-aware).
        ip_country (str): Code pays ISO 2 lettres (peut être None/vide).
        device_type (str): Type d'appareil ("mobile", "desktop", "tablet", "pos").
        velocity_1h (int): Nombre de transactions du client sur l'heure glissante.
        velocity_24h (int): Nombre de transactions du client sur les 24h glissantes.

    Returns:
        np.ndarray: Vecteur 1D de longueur len(FEATURE_NAMES), dtype float32.
    """
    # log1p plutôt que le montant brut : la distribution des montants est
    # très asymétrique (beaucoup de petites transactions, quelques grosses) —
    # log1p compresse l'échelle et évite que les gros montants écrasent le
    # signal des règles à seuil bas (ex. card testing).
    amount_log = np.log1p(float(amount))

    if created_at is None:
        created_at = datetime.now(timezone.utc)
    hour_of_day = float(created_at.hour)
    day_of_week = float(created_at.weekday())

    is_high_risk_country = 1.0 if (ip_country or "") in HIGH_RISK_COUNTRIES else 0.0
    is_pos_device = 1.0 if device_type == "pos" else 0.0

    return np.array([
        amount_log,
        hour_of_day,
        day_of_week,
        is_high_risk_country,
        is_pos_device,
        float(velocity_1h or 0),
        float(velocity_24h or 0),
    ], dtype=np.float32)
