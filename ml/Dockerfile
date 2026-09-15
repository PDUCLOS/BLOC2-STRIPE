# Image du service ml-monitor : boucle Evidently (drift + perf) + déclenchement
# auto du réentraînement. Build context = racine du projet (comme dashboard/Dockerfile)
# pour avoir accès à requirements.txt, _env.py et ml/.

FROM python:3.11-slim

# Sans ça, les print() de ml/monitor.py restent bloqués dans le buffer stdout
# de Python tant qu'il n'est pas plein — `docker logs` en direct resterait
# vide (ou très en retard) malgré une boucle qui tourne réellement.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# gcc/libpq-dev : compilation psycopg2 ; libgomp1 : requis par XGBoost (OpenMP)
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends gcc libpq-dev libgomp1 && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# _env.py à la racine de /app : ml/train_fraud_model.py et ml/monitor.py font
# sys.path.insert(0, parent.parent) puis `import _env`, même pattern que dashboard/.
COPY _env.py /app/_env.py
COPY ml/ /app/ml/

# Le modèle entraîné et ses métadonnées vivent dans ml/models/ (gitignored,
# généré par make ml-train) — le conteneur les lit/écrit au même endroit que
# le scorer producers/flink_like_job.py sur le host, via ce volume monté.

CMD ["python", "-m", "ml.monitor"]
