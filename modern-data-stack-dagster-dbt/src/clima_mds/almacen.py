"""Conexión a DuckDB con acceso a MinIO (API S3) y conversión de los CSV de NOAA a Parquet."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd

from clima_mds import configuracion as cfg

# Columnas del CSV por año de GHCN-Daily (no trae cabecera). Ver readme-by_year.txt de NOAA.
COLUMNAS_CSV = {
    "id": "VARCHAR",
    "fecha": "INTEGER",  # AAAAMMDD
    "elemento": "VARCHAR",
    "valor": "INTEGER",  # décimas de °C / décimas de mm según el elemento
    "m_flag": "VARCHAR",
    "q_flag": "VARCHAR",
    "s_flag": "VARCHAR",
    "hora_obs": "VARCHAR",
}


def conectar(ruta: str = ":memory:", s3: bool = True, solo_lectura: bool = False) -> duckdb.DuckDBPyConnection:
    """Conexión DuckDB con memoria acotada, derrame a disco y secreto S3 apuntando a MinIO."""
    con = duckdb.connect(ruta, read_only=solo_lectura)
    temporal = Path(os.environ.get("DIR_TEMPORAL_DUCKDB", "/tmp/duckdb_temporal"))
    temporal.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET memory_limit = '{cfg.MEMORIA_DUCKDB}'")
    con.execute(f"SET temp_directory = '{temporal.as_posix()}'")
    con.execute("SET threads = 4")
    con.execute("SET preserve_insertion_order = false")
    if s3:
        con.execute("LOAD httpfs")
        con.execute(
            f"""CREATE OR REPLACE SECRET minio (TYPE s3, KEY_ID '{cfg.S3_ACCESS_KEY}', SECRET '{cfg.S3_SECRET_KEY}',
                ENDPOINT '{cfg.S3_ENDPOINT}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')"""
        )
    return con


def sql_leer_csv(ruta_gz: str) -> str:
    columnas = ", ".join(f"'{k}': '{v}'" for k, v in COLUMNAS_CSV.items())
    return (f"read_csv('{ruta_gz}', header = false, delim = ',', quote = '', compression = 'gzip', "
            f"columns = {{{columnas}}})")


def csv_a_parquet(con, ruta_gz: str, destino: str, simular_datos_malos: bool = False) -> None:
    """Convierte un año de CSV comprimido a Parquet (zstd) ordenado por elemento y estación.

    Ordenar por elemento hace que los filtros de dbt (solo TMAX, TMIN, PRCP) salten grupos de filas enteros
    gracias a las estadísticas min/max del Parquet. `simular_datos_malos` corrompe ~2 % de las TMAX
    (999,9 °C) para demostrar que los controles de calidad detienen el pipeline.
    """
    valor = (
        "CASE WHEN elemento = 'TMAX' AND hash(id) % 50 = 0 THEN 9999 ELSE valor END"
        if simular_datos_malos
        else "valor"
    )
    con.execute(
        f"""COPY (
              SELECT id, fecha, elemento, {valor} AS valor, m_flag, q_flag, s_flag, hora_obs
              FROM {sql_leer_csv(ruta_gz)}
              ORDER BY elemento, id, fecha
            ) TO '{destino}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 500000)"""
    )


def tamano_objeto_s3(con, ruta: str) -> int:
    """Bytes del objeto en MinIO (read_blob solo pide el tamaño si no se proyecta el contenido)."""
    return int(con.execute("SELECT size FROM read_blob(?)", [ruta]).fetchone()[0])


# ---------- metadatos de estaciones y países (texto de ancho fijo) ----------

def parsear_estaciones(texto: str) -> pd.DataFrame:
    """ghcnd-stations.txt: ID 1-11, LAT 13-20, LON 22-30, ELEV 32-37, ESTADO 39-40, NOMBRE 42-71,
    GSN 73-75, HCN/CRN 77-79, WMO 81-85 (posiciones de NOAA, base 1)."""
    filas = []
    for linea in texto.splitlines():
        if len(linea) < 40:
            continue
        filas.append(
            {
                "id": linea[0:11].strip(),
                "latitud": float(linea[12:20]),
                "longitud": float(linea[21:30]),
                "elevacion_m": float(linea[31:37]),
                "estado": linea[38:40].strip() or None,
                "nombre": linea[41:71].strip(),
                "gsn": linea[72:75].strip() or None,
                "hcn_crn": linea[76:79].strip() or None,
                "wmo_id": linea[80:85].strip() or None,
            }
        )
    return pd.DataFrame(filas)


def parsear_paises(texto: str) -> pd.DataFrame:
    """ghcnd-countries.txt: CÓDIGO 1-2, NOMBRE 4-64."""
    filas = [
        {"codigo": linea[0:2], "nombre": linea[3:].strip()} for linea in texto.splitlines() if len(linea) >= 4
    ]
    return pd.DataFrame(filas)


def df_a_parquet(con, df: pd.DataFrame, destino: str) -> int:
    con.register("df_temporal", df)
    con.execute(f"COPY df_temporal TO '{destino}' (FORMAT parquet, COMPRESSION zstd)")
    con.unregister("df_temporal")
    return len(df)
