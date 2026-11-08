"""Carga horas de pageviews en las capas crudo y limpio de una rama de Nessie (idempotente por hora).

Uso (dentro del contenedor `cargador`):
  python scripts/cargar.py 2026-07-18 2026-07-19 --rama main
  python scripts/cargar.py 2026-07-21 --rama dev --capas limpio --defectos   # caso que debe fallar
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from lakehouse.catalogo import catalogo  # noqa: E402
from lakehouse.descarga import descargar_hora, horas_de_dias  # noqa: E402
from lakehouse.tablas import (  # noqa: E402  # noqa: E402
    CRUDO,
    LIMPIO,
    PROP_HORA,
    crear_tablas,
    horas_cargadas,
    leer_lineas,
    tabla_crudo,
    tabla_limpia,
)

# Defectos inyectados en el caso que debe fallar: faltan 3 horas, una hora se carga dos veces y se cuelan
# páginas especiales (como si un proceso nuevo se hubiera saltado la limpieza).
HORAS_FALTANTES = {3, 4, 5}
HORA_DUPLICADA = 12
HORA_CON_ESPECIALES = 13


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("dias", nargs="+")
    p.add_argument("--rama", default="main")
    p.add_argument("--capas", default="crudo,limpio")
    p.add_argument("--defectos", action="store_true")
    a = p.parse_args()
    capas = set(a.capas.split(","))
    cat = catalogo(a.rama)
    crear_tablas(cat)
    t_crudo, t_limpio = cat.load_table(CRUDO), cat.load_table(LIMPIO)
    hechas = {"crudo": horas_cargadas(t_crudo), "limpio": horas_cargadas(t_limpio)}
    lote = datetime.now(UTC).strftime(f"{a.rama}-%Y%m%dT%H%M%S")
    registros = []
    horas = []
    for h in horas_de_dias(a.dias):
        clave = f"{h:%Y-%m-%dT%H:00}"
        pendientes = [c for c in ("crudo", "limpio") if c in capas and clave not in hechas[c]]
        if a.defectos and h.hour in HORAS_FALTANTES:
            continue
        if not pendientes:
            print(f"{clave} ya cargada en {a.rama}", flush=True)
            continue
        horas.append((h, clave, pendientes))
    # Mientras se procesa una hora se descarga la siguiente: nunca hay más de una conexión abierta al servidor.
    bajador = ThreadPoolExecutor(max_workers=1)
    futuros = {i: bajador.submit(descargar_hora, h, RAIZ / "data" / "pageviews") for i, (h, _, _) in
               enumerate(horas[:1])}
    for i, (h, clave, pendientes) in enumerate(horas):
        t0 = time.perf_counter()
        d = futuros.pop(i).result()
        t_desc = time.perf_counter() - t0
        if i + 1 < len(horas):
            futuros[i + 1] = bajador.submit(descargar_hora, horas[i + 1][0], RAIZ / "data" / "pageviews")
        lineas = leer_lineas(RAIZ / "data" / "pageviews" / d.archivo)
        reg = {"hora": clave, "rama": a.rama, "lineas": len(lineas), "espera_descarga_s": round(t_desc, 1)}
        props = {PROP_HORA: clave, "lakehouse.lote": lote}
        if "crudo" in pendientes:
            t1 = time.perf_counter()
            t_crudo.append(tabla_crudo(lineas, h, d.archivo), snapshot_properties=props)
            reg["crudo_s"] = round(time.perf_counter() - t1, 1)
        if "limpio" in pendientes:
            t1 = time.perf_counter()
            t_limpio = cat.load_table(LIMPIO)  # recoge cambios de esquema o de partición hechos desde Trino
            con_lote = "lote" in t_limpio.schema().column_names
            tabla, res = tabla_limpia(lineas, h, lote if con_lote else None)
            if a.defectos and h.hour == HORA_CON_ESPECIALES:
                tabla = _con_especiales(tabla)
            t_limpio.append(tabla, snapshot_properties=props)
            if a.defectos and h.hour == HORA_DUPLICADA:
                t_limpio.append(tabla, snapshot_properties={**props, PROP_HORA: clave + "-repetida"})
            reg.update(res.resumen())
            reg["limpio_s"] = round(time.perf_counter() - t1, 1)
        registros.append(reg)
        print(json.dumps(reg, ensure_ascii=False), flush=True)
    salida = RAIZ / "reports" / "carga"
    salida.mkdir(parents=True, exist_ok=True)
    nombre = f"{'_'.join(a.dias)}_{a.rama}{'_defectos' if a.defectos else ''}.json"
    previos = json.loads((salida / nombre).read_text(encoding="utf-8")) if (salida / nombre).exists() else []
    (salida / nombre).write_text(json.dumps(previos + registros, indent=1, ensure_ascii=False), encoding="utf-8")


def _con_especiales(tabla):
    import pyarrow as pa

    extra = tabla.slice(0, 50)
    titulos = pa.array([f"Special:Search/{i}" for i in range(50)], pa.string())
    extra = extra.set_column(extra.schema.get_field_index("titulo"), "titulo", titulos)
    return pa.concat_tables([tabla, extra])


if __name__ == "__main__":
    main()
