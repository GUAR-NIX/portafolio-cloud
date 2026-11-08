# Portafolio de Ingeniería de datos y Cloud

Streaming, orquestación, modern data stack, serverless, Kubernetes y lakehouse con Iceberg, ejecutados en local con Docker y con infraestructura como código lista para la nube.

**Herramientas principales:** Docker · Spark · Airflow · Dagster · dbt · Kubernetes · Terraform · Iceberg

Cada carpeta es un proyecto independiente, con su propio README (resultados primero), guía paso a paso, pruebas automáticas y cómo reproducirlo. Los datos crudos no se versionan: cada proyecto trae un script que los descarga desde la fuente oficial.

## Proyectos

| Proyecto | De qué trata |
|---|---|
| [**Ingesta diaria de APIs públicas con Apache Airflow · clima del Ecuador + Banco Mundial**](airflow-apis-postgres/) | Orquestación con Apache Airflow 3.3 en Docker Compose: tres DAG que traen el clima diario de 13 ciudades del Ecuador (Open-Meteo) y 10 indicadores del Banco Mundial para Ecuador y 7 países de la región, y los cargan en Postgres… |
| [**☁️ API de clima del Ecuador en Kubernetes · FastAPI, Helm y observabilidad**](kubernetes-api-clima/) | API FastAPI y aplicación Streamlit para consultar 4.392 observaciones diarias de clima de 12 ciudades del Ecuador durante 2024, desplegadas en un clúster kind local con Helm, Ingress, HPA, metrics-server, Prometheus y Grafana.… |
| [**🧊 Lakehouse local con Apache Iceberg + Trino + Nessie sobre los pageviews de Wikipedia**](lakehouse-iceberg-trino-nessie/) | Lakehouse completo en Docker Compose: MinIO como S3, Nessie como catálogo Iceberg REST con ramas tipo git, Trino como motor SQL y un cargador en Python (PyIceberg). Ingiere 96 dumps horarios de pageviews de Wikipedia (18 al 21 de… |
| [**🌦️ Modern data stack local: Dagster + dbt + DuckDB + MinIO sobre NOAA GHCN-Daily**](modern-data-stack-dagster-dbt/) | Pipeline orquestado por activos que ingiere 177,5 millones de observaciones diarias de ~45.000 estaciones meteorológicas del mundo (NOAA GHCN-Daily, años 2022–2026, 6,4 GB de CSV), las guarda en un lago tipo S3 (MinIO, Parquet) y… |
| [**🌎 Alertas serverless de sismos · USGS → S3 → Lambda → DynamoDB → SNS**](serverless-sismos-localstack/) | Pipeline orientado a eventos que valida GeoJSON del USGS, guarda cada sismo una sola vez y emite una alerta para eventos de magnitud ≥ 5 a menos de 1.500 km del centro de Ecuador. La arquitectura AWS se despliega con Terraform… |
| [**Streaming en tiempo real de la actividad de GitHub · Redpanda + Spark Structured Streaming + Delta Lake**](streaming-github-redpanda-spark/) | Plataforma de streaming 100 % local en Docker: 11,4 millones de eventos públicos de GitHub (GH Archive, 16–18 sep 2025) se reproducen hacia Redpanda (Kafka) respetando sus marcas de tiempo, y Spark Structured Streaming los lleva… |

## Cómo usar este repositorio

```bash
git clone https://github.com/GUAR-NIX/portafolio-cloud.git
cd portafolio-cloud/<proyecto>
```

Dentro de cada proyecto, sigue la sección «Cómo reproducir» de su README y `docs/GUIA_PASO_A_PASO.md`.

## Autor

**Erick Guarnizo** · [LinkedIn](https://www.linkedin.com/in/erick-g-571a7b3b4)

## Licencia

El código está bajo licencia [MIT](LICENSE). Los datos de cada proyecto conservan la licencia de su fuente, indicada en su README.
