# Imagen única del proyecto: Python 3.11 + Java 17 + PySpark 3.5.3 con los conectores ya descargados
# (Kafka, Delta Lake 3.2.1, S3A para MinIO). La usan los servicios spark, productor, dashboard y pruebas.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

COPY docker/spark/precargar_jars.py /tmp/precargar_jars.py
RUN python /tmp/precargar_jars.py && rm /tmp/precargar_jars.py

# El código se monta como volumen en /app (docker-compose.yml); así no hay que reconstruir al editarlo.
ENV PYTHONPATH=/app/src
WORKDIR /app
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "--version"]
