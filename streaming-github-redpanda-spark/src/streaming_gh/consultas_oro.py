"""Consultas de negocio sobre las tablas oro con DuckDB (extensiones delta + httpfs), leyendo directo de MinIO.

Sin Spark ni clúster: DuckDB lee el log de Delta y los Parquet por S3. Es lo que usaría un analista o un tablero.

    python -m streaming_gh.consultas_oro --corrida principal    → reports/consultas/*.csv
"""

from __future__ import annotations

import argparse
import contextlib
import sys
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import pandas as pd

from streaming_gh.config import BUCKET, RAIZ, S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY, TABLAS

DIR_SQL = RAIZ / "sql"
DIR_SALIDA = RAIZ / "reports" / "consultas"


def conectar(corrida: str, memoria: str | None = None) -> duckdb.DuckDBPyConnection:
    """Conexión con vistas sobre las tablas Delta de la corrida. `memoria` acota DuckDB (desborda a disco)."""
    con = duckdb.connect()
    if memoria:
        con.execute(f"SET memory_limit = '{memoria}'; SET temp_directory = '/tmp/duckdb'; SET threads = 2;")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL delta; LOAD delta;")
    url = urlparse(S3_ENDPOINT)
    con.execute(f"""
        CREATE SECRET minio (TYPE S3, KEY_ID '{S3_ACCESS_KEY}', SECRET '{S3_SECRET_KEY}', ENDPOINT '{url.netloc}',
                             URL_STYLE 'path', USE_SSL {str(url.scheme == 'https').lower()}, REGION 'us-east-1')
    """)
    for capa, tablas in TABLAS.items():
        for tabla in tablas:
            nombre = tabla if capa == "oro" else capa  # vistas: actividad_5min, repos_hora, ..., bronce, plata
            ruta = f"s3://{BUCKET}/{corrida}/{capa}/{tabla}"
            with contextlib.suppress(duckdb.Error):  # p. ej. la tabla de picos no existe si nunca hubo uno
                con.execute(f"CREATE VIEW {nombre} AS SELECT * FROM delta_scan('{ruta}')")
    return con


def consultas_disponibles() -> list[Path]:
    return sorted(DIR_SQL.glob("*.sql"))


def ejecutar(con: duckdb.DuckDBPyConnection, archivo: Path) -> pd.DataFrame:
    return con.execute(archivo.read_text(encoding="utf-8")).df()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corrida", default="principal")
    args = parser.parse_args()
    con = conectar(args.corrida)
    DIR_SALIDA.mkdir(parents=True, exist_ok=True)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)
    for archivo in consultas_disponibles():
        try:
            df = ejecutar(con, archivo)
        except duckdb.Error as error:
            print(f"\n== {archivo.name}: {error}")
            continue
        df.to_csv(DIR_SALIDA / archivo.with_suffix(".csv").name, index=False)
        print(f"\n== {archivo.name} ({len(df)} filas)\n{df.head(15).to_string(index=False)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
