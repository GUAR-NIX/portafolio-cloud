"""Ejecución de SQL en Trino con las estadísticas de cada consulta (tiempo, bytes leídos, filas)."""

from __future__ import annotations

import json
from pathlib import Path

CAMPOS_STATS = ("elapsedTimeMillis", "cpuTimeMillis", "physicalInputBytes", "processedRows", "processedBytes",
                "totalSplits", "peakMemoryBytes")


def ejecutar(con, sql: str) -> tuple[list[tuple], dict]:
    cur = con.cursor()
    cur.execute(sql)
    filas = cur.fetchall()
    stats = {k: cur.stats.get(k) for k in CAMPOS_STATS}
    stats["query_id"] = cur.query_id
    stats["columnas"] = [d[0] for d in (cur.description or [])]
    return filas, stats


def filas_dict(con, sql: str) -> list[dict]:
    filas, stats = ejecutar(con, sql)
    return [dict(zip(stats["columnas"], f, strict=True)) for f in filas]


def uno(con, sql: str):
    return ejecutar(con, sql)[0][0][0]


def guardar(ruta: Path, datos) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text(json.dumps(datos, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
