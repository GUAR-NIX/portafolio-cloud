"""Write-audit-publish con ramas de Nessie: cargar un día en `dev`, auditarlo con controles y fusionar a `main`
solo si pasa. Primero se prueba con defectos inyectados (no se fusiona) y luego con el día correcto.

  python scripts/ramas.py   # dentro del contenedor `cargador`
Deja la evidencia en reports/iceberg/05_ramas_nessie.json.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from lakehouse import configuracion as cfg  # noqa: E402
from lakehouse.catalogo import trino  # noqa: E402
from lakehouse.consultas import ejecutar, guardar, uno  # noqa: E402
from lakehouse.controles import aprobado, como_dict, evaluar, sql_metricas  # noqa: E402
from lakehouse.nessie import Nessie  # noqa: E402

DIA = cfg.DIA_RAMA


def metricas(con, catalogo: str, dia: str) -> dict:
    q = sql_metricas(f"{catalogo}.wiki.pageviews_limpio", dia)
    filas, stats = ejecutar(con, q["generales"])
    m = dict(zip(stats["columnas"], filas[0], strict=True))
    m["vistas_por_idioma"] = {i: v for i, v in ejecutar(con, q["por_idioma"])[0]}
    return m


def cargar(*args: str) -> float:
    t0 = time.perf_counter()
    subprocess.run([sys.executable, str(RAIZ / "scripts" / "cargar.py"), DIA, "--rama", "dev", *args], check=True)
    return round(time.perf_counter() - t0, 1)


def resumen_main(con) -> dict:
    return {"hash": Nessie(os.environ["NESSIE_URI"]).hash("main"),
            "filas_dia": uno(con, "SELECT count(*) FROM iceberg.wiki.pageviews_limpio "
                                  f"WHERE CAST(hora AS date) = DATE '{DIA}'"),
            "filas_crudo_dia": uno(con, "SELECT count(*) FROM iceberg.wiki.pageviews_crudo "
                                        f"WHERE CAST(hora AS date) = DATE '{DIA}'")}


def auditar(con, caso: str, ns: Nessie) -> dict:
    referencia = [metricas(con, "iceberg", d) for d in cfg.DIAS_MAIN]
    candidato = metricas(con, "iceberg_dev", DIA)
    controles = evaluar(candidato, referencia)
    ok = aprobado(controles)
    return {"caso": caso, "metricas_candidato": candidato,
            "referencia_vistas_por_idioma": [r["vistas_por_idioma"] for r in referencia],
            "controles": como_dict(controles), "aprobado": ok,
            "diferencias_main_dev": ns.diferencias("main", "dev")}


def main() -> None:
    ns = Nessie(os.environ["NESSIE_URI"])
    con = trino()
    evidencia = {"dia": DIA}

    # 1) Caso que FALLA: el día llega incompleto, con una hora repetida y páginas especiales
    ns.reasignar("dev", "main")
    antes = resumen_main(con)
    seg = cargar("--capas", "limpio", "--defectos")
    r = auditar(con, "con defectos", ns)
    r["segundos_carga"] = seg
    if r["aprobado"]:
        raise SystemExit("los controles deberían haber fallado")
    r["accion"] = "NO se fusiona; dev se reasigna a main (los datos malos nunca llegan a main)"
    r["main_antes"], r["main_despues"] = antes, resumen_main(con)
    r["dev_hash_descartado"] = ns.hash("dev")
    ns.reasignar("dev", "main")
    evidencia["caso_falla"] = r

    # 2) Caso que PASA: el mismo día bien cargado (crudo + limpio)
    seg = cargar()
    r = auditar(con, "correcto", ns)
    r["segundos_carga"] = seg
    if not r["aprobado"]:
        raise SystemExit(f"el día correcto no pasó los controles: {r['controles']}")
    r["main_antes"] = resumen_main(con)
    t0 = time.perf_counter()
    r["merge"] = ns.fusionar("dev", "main", f"Día {DIA} auditado en dev: {len(r['controles'])} controles OK")
    r["segundos_merge"] = round(time.perf_counter() - t0, 2)
    r["main_despues"] = resumen_main(con)
    evidencia["caso_pasa"] = r
    evidencia["historial_main"] = [{k: c.get(k) for k in ("hash", "message", "commitTime")}
                                   for c in ns.historial("main", 8)]
    evidencia["ramas"] = ns.ramas()
    guardar(RAIZ / "reports" / "iceberg" / "05_ramas_nessie.json", evidencia)
    for caso in ("caso_falla", "caso_pasa"):
        c = evidencia[caso]
        print(caso, c["aprobado"], [x["nombre"] for x in c["controles"] if not x["paso"]],
              c["main_antes"], c["main_despues"])


if __name__ == "__main__":
    main()
