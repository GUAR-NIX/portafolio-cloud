# Imagen única para Dagster (webserver + daemon) con dbt-duckdb dentro.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DAGSTER_HOME=/opt/dagster/home \
    PYTHONPATH=/opt/proyecto/src \
    HOME=/root

COPY requirements.txt requirements-dev.txt /tmp/
RUN pip install -r /tmp/requirements-dev.txt \
 && python -c "import duckdb; duckdb.sql('INSTALL httpfs')"

COPY docker/dagster.yaml /opt/dagster/home/dagster.yaml
WORKDIR /opt/proyecto
