"""Configuración leída de variables de entorno (con valores por defecto para la PC local)."""

import os
from pathlib import Path

RAIZ = Path(os.environ.get("RAIZ_PROYECTO", Path(__file__).resolve().parents[2]))
DIR_CRUDO = RAIZ / "data" / "raw"
DIR_REPORTES = RAIZ / "reports"
DIR_DBT = RAIZ / "dbt_clima"

URL_BASE_NOAA = "https://noaa-ghcn-pds.s3.amazonaws.com"
ANIOS = [a.strip() for a in os.environ.get("ANIOS", "2022,2023,2024,2025,2026").split(",") if a.strip()]

BUCKET = os.environ.get("BUCKET", "ghcn")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minio_local")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minio_local_clave")
RUTA_DUCKDB = os.environ.get("RUTA_DUCKDB", str(RAIZ / "data" / "clima.duckdb"))
MEMORIA_DUCKDB = os.environ.get("MEMORIA_DUCKDB", "2500MB")


def ruta_s3_crudo(anio: str) -> str:
    """Ubicación del Parquet crudo de un año, con particionado tipo Hive (anio=AAAA)."""
    return f"s3://{BUCKET}/crudo/ghcn_diario/anio={anio}/datos.parquet"


def ruta_s3_metadatos(nombre: str) -> str:
    return f"s3://{BUCKET}/crudo/metadatos/{nombre}.parquet"
