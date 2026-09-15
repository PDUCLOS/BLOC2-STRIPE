# Image Streamlit pour le dashboard de démo Stripe Polyglot
# Build context = racine du projet (besoin de requirements.txt + _env.py + dashboard/)

FROM python:3.11-slim

WORKDIR /app

# libpq-dev + gcc requis pour compiler psycopg2 ; curl pour le healthcheck
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends gcc libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# _env.py doit rester à la racine de /app car dashboard/app.py fait
# sys.path.insert(0, parent.parent) puis `import _env`
COPY _env.py /app/_env.py
COPY dashboard/ /app/dashboard/

EXPOSE 8501

HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD curl -fsS http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
