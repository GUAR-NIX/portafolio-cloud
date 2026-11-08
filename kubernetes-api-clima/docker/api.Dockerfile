FROM python:3.12.15-slim AS dependencias
WORKDIR /instalacion
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/instalacion/paquetes -r requirements.txt

FROM python:3.12.15-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app/src RUTA_DATOS=/app/data/clima_ecuador.parquet
RUN useradd --create-home --uid 10001 api
WORKDIR /app
COPY --from=dependencias /instalacion/paquetes /usr/local
COPY src ./src
COPY data/procesados/clima_ecuador.parquet ./data/clima_ecuador.parquet
USER 10001
EXPOSE 8000
CMD ["uvicorn", "clima_api.principal:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--timeout-graceful-shutdown", "10"]

