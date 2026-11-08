"""Descarga los días configurados (o los que se pasen) a data/pageviews/, una hora a la vez.

Uso:  python scripts/descargar.py [AAAA-MM-DD ...] [--pausa 2]
"""

import argparse
import json
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from lakehouse import configuracion as cfg  # noqa: E402
from lakehouse.descarga import MIN_VISTAS, descargar_hora, horas_de_dias  # noqa: E402

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("dias", nargs="*", default=cfg.DIAS_MAIN + [cfg.DIA_RAMA])
    p.add_argument("--pausa", type=float, default=2.0, help="segundos entre archivos (respeto al servidor)")
    a = p.parse_args()
    destino = RAIZ / "data" / "pageviews"
    resumen = []
    t0 = time.perf_counter()
    for h in horas_de_dias(a.dias):
        r = descargar_hora(h, destino)
        resumen.append(r)
        estado = "caché" if r.reutilizado else f"{r.segundos:.0f} s"
        print(f"{r.hora}  origen {r.bytes_origen / 1e6:5.1f} MB  {r.lineas_origen:>9,} líneas  →  "
              f"{r.lineas_guardadas:>9,} es/en ≥{MIN_VISTAS}  {r.bytes_guardados / 1e6:5.1f} MB  ({estado})",
              flush=True)
        if not r.reutilizado:
            time.sleep(a.pausa)
    total = {
        "dias": a.dias,
        "archivos": len(resumen),
        "bytes_origen": sum(r.bytes_origen for r in resumen),
        "bytes_guardados": sum(r.bytes_guardados for r in resumen),
        "lineas_origen": sum(r.lineas_origen for r in resumen),
        "lineas_guardadas": sum(r.lineas_guardadas for r in resumen),
        "vistas_en_es": sum(r.vistas_proyectos for r in resumen),
        "vistas_guardadas": sum(r.vistas_guardadas for r in resumen),
        "segundos_esta_corrida": round(time.perf_counter() - t0, 1),
    }
    total["pct_vistas_guardadas"] = round(100 * total["vistas_guardadas"] / max(total["vistas_en_es"], 1), 2)
    total["umbral_vistas"] = MIN_VISTAS
    (RAIZ / "reports").mkdir(exist_ok=True)
    (RAIZ / "reports" / "descarga.json").write_text(json.dumps(total, indent=2), encoding="utf-8")
    print(json.dumps(total, indent=2))
