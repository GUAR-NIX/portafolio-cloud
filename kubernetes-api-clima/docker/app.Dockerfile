FROM python:3.12.15-slim AS dependencias
WORKDIR /instalacion
COPY requirements-app.txt .
RUN pip install --no-cache-dir --prefix=/instalacion/paquetes -r requirements-app.txt

FROM python:3.12.15-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 API_URL=http://clima-api:8000
RUN useradd --create-home --uid 10001 app
WORKDIR /app
COPY --from=dependencias /instalacion/paquetes /usr/local
COPY app/app.py ./app.py
USER 10001
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]

