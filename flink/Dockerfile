# Image Flink custom avec PyFlink + connecteurs Kafka/Redis/Mongo
# Basée sur l'image officielle Flink 1.18 (Java 11)

FROM flink:1.18-java11

# Switch to root pour installer les dépendances Python
USER root

# Installation Python + JDK headers (nécessaires pour compiler apache-flink-libraries)
RUN apt-get update -y && \
    apt-get install -y python3.11 python3-pip python3.11-venv netcat-openbsd curl \
                       default-jdk-headless python3.11-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Symlink pour que `python` pointe sur 3.11
RUN ln -sf /usr/bin/python3.11 /usr/bin/python && \
    ln -sf /usr/bin/python3.11 /usr/bin/python3

# Hack : apache-flink-libraries cherche /opt/java/openjdk/include qui n'existe pas.
# L'image Flink utilise Adoptium Temurin dans /opt/java/openjdk mais sans les includes.
# On lie les includes du JDK Debian vers /opt/java/openjdk pour que la compilation trouve
# les headers JNI.
RUN if [ -d /opt/java/openjdk ] && [ ! -d /opt/java/openjdk/include ]; then \
        JDK_DIR=$(ls -d /usr/lib/jvm/java-11-openjdk-* 2>/dev/null | head -1); \
        if [ -z "$JDK_DIR" ]; then \
            JDK_DIR=$(ls -d /usr/lib/jvm/default-java 2>/dev/null | head -1); \
        fi; \
        if [ -n "$JDK_DIR" ] && [ -d "$JDK_DIR/include" ]; then \
            ln -sf "$JDK_DIR/include" /opt/java/openjdk/include; \
            echo "Linked $JDK_DIR/include -> /opt/java/openjdk/include"; \
        else \
            echo "WARNING: Could not find JDK include dir"; \
        fi; \
    fi

# IMPORTANT : installer numpy AVANT apache-flink pour bypass le résolveur de pip
# qui veut mettre numpy==1.21.4 (incompatible Python 3.11)
# Le trick : on installe apache-flink SANS ses dépendances, puis on les installe
# une par une avec les versions compatibles
RUN pip3 install --no-cache-dir "numpy>=1.26.0,<2.0" && \
    pip3 install --no-cache-dir "pyarrow" "fastavro" "cryptography" "cloudpickle" "py4j" "jpype1" "pemja" && \
    pip3 install --no-cache-dir "apache-flink==1.18.1"

# Copie requirements et installe le reste
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Télécharge les JARs pour PyFlink
ENV FLINK_LIB_DIR=/opt/flink/lib
RUN mkdir -p ${FLINK_LIB_DIR}

# Kafka connector JAR (PyFlink ne supporte pas Kafka nativement, il faut le jar)
ARG FLINK_KAFKA_JAR=https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.2-1.18/flink-connector-kafka-3.0.2-1.18.jar
RUN curl -fsSL -o ${FLINK_LIB_DIR}/flink-connector-kafka-3.0.2-1.18.jar ${FLINK_KAFKA_JAR}

# Kafka clients (nécessaire pour le connecteur Kafka, non-bundlé)
ARG KAFKA_CLIENTS_JAR=https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar
RUN curl -fsSL -o ${FLINK_LIB_DIR}/kafka-clients-3.4.0.jar ${KAFKA_CLIENTS_JAR}

# Retour à l'utilisateur flink
USER flink

# Dossier jobs
RUN mkdir -p /opt/flink/jobs
WORKDIR /opt/flink/jobs
