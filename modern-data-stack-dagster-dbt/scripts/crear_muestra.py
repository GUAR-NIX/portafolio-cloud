"""Crea data/muestra/ (versionada, < 2 MB) a partir del Parquet real de MinIO, para CI y pruebas.

Toma 3 estaciones por país (las de más días con TMAX en 2023) en 12 países y los años 2022-2025,
con el mismo particionado que MinIO: ghcn_diario/anio=AAAA/datos.parquet y metadatos/*.parquet.
Uso (con el stack levantado):  docker compose exec dagster-daemon python scripts/crear_muestra.py
"""

import json

from clima_mds import almacen
from clima_mds import configuracion as cfg

PAISES = ["EC", "CO", "PE", "BR", "AR", "MX", "US", "CA", "SP", "GM", "AS", "JA"]
ANIOS = [2022, 2023, 2024, 2025]
DESTINO = cfg.RAIZ / "data" / "muestra"


def main() -> None:
    con = almacen.conectar()
    crudo = f"read_parquet('s3://{cfg.BUCKET}/crudo/ghcn_diario/*/*.parquet', hive_partitioning = true)"
    lista = ", ".join(f"'{p}'" for p in PAISES)
    con.execute(
        f"""CREATE TEMP TABLE elegidas AS
            SELECT id FROM (
                SELECT id, row_number() OVER (PARTITION BY substr(id, 1, 2) ORDER BY count(*) DESC, id) AS n
                FROM {crudo}
                WHERE anio = 2023 AND elemento IN ('TMAX', 'TMIN') AND substr(id, 1, 2) IN ({lista})
                GROUP BY id HAVING count(DISTINCT elemento) = 2
            ) WHERE n <= 3"""
    )
    for anio in ANIOS:
        carpeta = DESTINO / "ghcn_diario" / f"anio={anio}"
        carpeta.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"""COPY (SELECT id, fecha, elemento, valor, m_flag, q_flag, s_flag, hora_obs
                      FROM {crudo}
                      WHERE anio = {anio} AND id IN (SELECT id FROM elegidas)
                        AND elemento IN ('TMAX', 'TMIN', 'PRCP', 'SNWD')
                      ORDER BY elemento, id, fecha)
                TO '{(carpeta / 'datos.parquet').as_posix()}' (FORMAT parquet, COMPRESSION zstd)"""
        )
    (DESTINO / "metadatos").mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""COPY (SELECT * FROM read_parquet('{cfg.ruta_s3_metadatos("estaciones")}')
                  WHERE id IN (SELECT id FROM elegidas))
            TO '{(DESTINO / 'metadatos' / 'estaciones.parquet').as_posix()}' (FORMAT parquet)"""
    )
    con.execute(
        f"""COPY (SELECT * FROM read_parquet('{cfg.ruta_s3_metadatos("paises")}'))
            TO '{(DESTINO / 'metadatos' / 'paises.parquet').as_posix()}' (FORMAT parquet)"""
    )
    filas, estaciones, con_qflag = con.execute(
        f"""SELECT count(*), count(DISTINCT id), count(q_flag)
            FROM read_parquet('{(DESTINO / 'ghcn_diario').as_posix()}/*/*.parquet')"""
    ).fetchone()
    resumen = {"filas": filas, "estaciones": estaciones, "filas_con_qflag": con_qflag, "paises": PAISES,
               "anios": ANIOS}
    (DESTINO / "LEEME.json").write_text(json.dumps(resumen, indent=2), encoding="utf-8")
    print(resumen)


if __name__ == "__main__":
    main()
