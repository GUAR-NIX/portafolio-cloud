"""Consultas en Trino con y sin poda (particiones y estadísticas de archivo): mediana de 5 corridas.

Cada caso pide lo mismo (top 10 de una hora en español) y solo cambia cómo se escribe el filtro o en qué parte de
la tabla cae la hora. Se registran tiempo, bytes físicos leídos, filas procesadas y splits.
"""

import statistics
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from lakehouse.catalogo import trino  # noqa: E402
from lakehouse.consultas import ejecutar, guardar  # noqa: E402

CORRIDAS = 5
L = "iceberg.wiki.pageviews_limpio"
C = "iceberg.wiki.pageviews_crudo"
TOP = "SELECT titulo, sum(vistas) AS v FROM {t} WHERE {f} GROUP BY titulo ORDER BY v DESC LIMIT 10"
TOP_CRUDO = ("SELECT titulo, sum(CAST(vistas AS bigint)) AS v FROM {t} WHERE {f} "
             "GROUP BY titulo ORDER BY v DESC LIMIT 10")

CASOS = {
    "limpio · hora en partición horaria (20-jul 19 h)": (
        TOP, L, "idioma = 'es' AND hora = TIMESTAMP '2026-07-20 19:00:00'"),
    "limpio · hora en partición diaria (19-jul 19 h)": (
        TOP, L, "idioma = 'es' AND hora = TIMESTAMP '2026-07-19 19:00:00'"),
    # Trino evalúa cualquier predicado sobre una columna de partición identidad (aun con funciones) para podar,
    # así que aquí solo se poda el idioma: la hora va envuelta en una función y no se usa.
    "limpio · solo poda por idioma (hora con función)": (
        TOP, L, "lower(idioma) = 'es' AND date_format(hora, '%Y-%m-%d %H') = '2026-07-20 19'"),
    # `dominio IN (...)` también poda: los min/max de cada archivo (partición en/es) lo descartan. Con funciones
    # sobre columnas que no son de partición no queda nada que usar y se leen todos los archivos.
    "limpio · sin poda (funciones sobre dominio y hora)": (
        TOP, L, "substr(dominio, 1, 2) = 'es' AND date_format(hora, '%Y-%m-%d %H') = '2026-07-20 19'"),
    "crudo compactado · hora (19-jul 19 h)": (
        TOP_CRUDO, C, "dominio IN ('es', 'es.m') AND hora = TIMESTAMP '2026-07-19 19:00:00'"),
    "crudo compactado · sin poda (19-jul 19 h)": (
        TOP_CRUDO, C, "dominio IN ('es', 'es.m') AND date_format(hora, '%Y-%m-%d %H') = '2026-07-19 19'"),
}


def main() -> None:
    con = trino()
    resultados = {}
    for nombre, (plantilla, tabla, filtro) in CASOS.items():
        sql = plantilla.format(t=tabla, f=filtro)
        ejecutar(con, sql)  # calentamiento (metadatos y conexión S3)
        corridas = [ejecutar(con, sql) for _ in range(CORRIDAS)]
        st = [s for _, s in corridas]

        def med(k, st=st):
            return statistics.median(s[k] for s in st)

        resultados[nombre] = {
            "sql": sql, "ms_mediana": med("elapsedTimeMillis"), "ms_corridas": [s["elapsedTimeMillis"] for s in st],
            "mb_leidos": round(med("physicalInputBytes") / 1e6, 2), "filas_procesadas": med("processedRows"),
            "splits": med("totalSplits"), "top1": list(corridas[0][0][0])}
        print(nombre, {k: resultados[nombre][k] for k in ("ms_mediana", "mb_leidos", "filas_procesadas", "splits")},
              flush=True)
    filas_tabla = ejecutar(con, f"SELECT count(*) FROM {L}")[0][0][0]
    guardar(RAIZ / "reports" / "rendimiento.json", {"corridas": CORRIDAS, "filas_limpio": filas_tabla,
                                                    "casos": resultados})


if __name__ == "__main__":
    main()
