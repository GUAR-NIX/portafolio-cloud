# Imagen oficial de Airflow + pytest para correr las pruebas en el mismo runtime que los DAG.
# El paquete `ingesta` y los DAG no se copian: se montan desde el proyecto (docker-compose.yml).
FROM apache/airflow:3.3.2-python3.12

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pytest>=8,<10"
