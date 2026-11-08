"""Crea la capa de agregados (sql/agregados.sql) en Trino y verifica que el corte de candidatos sea exacto."""

import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from lakehouse.catalogo import trino  # noqa: E402
from lakehouse.consultas import ejecutar, guardar, uno  # noqa: E402

UMBRAL = 50  # vistas de una fila (hora, dominio) para ser candidato


def sentencias(texto: str) -> list[str]:
    sin_comentarios = "\n".join(l for l in texto.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in sin_comentarios.split(";") if s.strip()]


def main() -> None:
    con = trino()
    pasos = []
    for s in sentencias((RAIZ / "sql" / "agregados.sql").read_text(encoding="utf-8")):
        tabla = s.split("TABLE", 1)[1].split()[0]
        t0 = time.perf_counter()
        _, stats = ejecutar(con, s)
        pasos.append({"tabla": tabla, "segundos": round(time.perf_counter() - t0, 1),
                      "filas": uno(con, f"SELECT count(*) FROM {tabla}"),
                      "bytes_leidos": stats["physicalInputBytes"]})
        print(pasos[-1], flush=True)
    # Un no candidato tiene todas sus filas < UMBRAL: suma < 2*UMBRAL por hora y < 2*UMBRAL*24 por día.
    min_top10 = uno(con, "SELECT min(vistas) FROM iceberg.wiki.agg_top_hora WHERE puesto = 10")
    min_top50 = uno(con, "SELECT min(vistas) FROM iceberg.wiki.agg_top_dia WHERE puesto = 50")
    verif = {"min_vistas_puesto10_hora": min_top10, "cota_no_candidato_hora": 2 * (UMBRAL - 1),
             "top_hora_exacto": min_top10 > 2 * (UMBRAL - 1),
             "min_vistas_puesto50_dia": min_top50, "cota_no_candidato_dia": 2 * (UMBRAL - 1) * 24,
             "top_dia_exacto": min_top50 > 2 * (UMBRAL - 1) * 24}
    guardar(RAIZ / "reports" / "agregados.json", {"pasos": pasos, "verificacion_corte": verif})
    print(verif)
    if not (verif["top_hora_exacto"] and verif["top_dia_exacto"]):
        raise SystemExit("el corte de candidatos podría cambiar algún top: bajar UMBRAL")


if __name__ == "__main__":
    main()
