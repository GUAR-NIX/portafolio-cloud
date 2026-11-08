"""Demostraciones de Iceberg con evidencia en reports/iceberg/ (se corren en orden, ver scripts/ejecutar_todo.sh).

  python scripts/demos_iceberg.py time_travel        # tras cargar 18 y 19 de julio
  python scripts/demos_iceberg.py esquema            # ADD COLUMN lote (solo metadatos)
  python scripts/demos_iceberg.py particiones_antes  # consultas de referencia y cambio de diaria a horaria
  python scripts/demos_iceberg.py particiones_despues   # tras cargar el 20 con la partición nueva
  python scripts/demos_iceberg.py compactacion       # optimize + expire_snapshots (luego nessie-gc en el host)
  python scripts/demos_iceberg.py compactacion_final # estado tras nessie-gc
"""

import json
import os
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from lakehouse.catalogo import catalogo, trino  # noqa: E402
from lakehouse.consultas import ejecutar, filas_dict, guardar, uno  # noqa: E402

SALIDA = RAIZ / "reports" / "iceberg"
LIMPIO = "iceberg.wiki.pageviews_limpio"
CRUDO = "iceberg.wiki.pageviews_crudo"

# Consultas "viejas": se guardan antes de cambiar la partición y se repiten después.
CONSULTAS_REFERENCIA = {
    "vistas_por_dia_e_idioma": f"""
        SELECT CAST(hora AS date) AS dia, idioma, sum(vistas) AS vistas, count(*) AS filas
        FROM {LIMPIO} WHERE hora < TIMESTAMP '2026-07-20 00:00:00' GROUP BY 1, 2 ORDER BY 1, 2""",
    "top5_en_final": f"""
        SELECT titulo, sum(vistas) AS vistas FROM {LIMPIO}
        WHERE idioma = 'en' AND hora >= TIMESTAMP '2026-07-19 19:00:00' AND hora < TIMESTAMP '2026-07-19 22:00:00'
        GROUP BY titulo ORDER BY vistas DESC, titulo LIMIT 5""",
}


def archivos(con, tabla: str) -> dict:
    return filas_dict(con, f"""
        SELECT count(*) AS archivos, sum(file_size_in_bytes) AS bytes, sum(record_count) AS filas,
               count_if(file_size_in_bytes < 16 * 1024 * 1024) AS archivos_menores_16mb,
               round(avg(file_size_in_bytes) / 1e6, 2) AS mb_promedio
        FROM "{tabla.split('.')[0]}"."{tabla.split('.')[1]}"."{tabla.split('.')[2]}$files" """)[0]


def snapshots(con, tabla: str) -> list[dict]:
    c, e, t = tabla.split(".")
    return filas_dict(con, f"""
        SELECT snapshot_id, committed_at, operation, element_at(summary, 'lakehouse.hora') AS hora_cargada,
               element_at(summary, 'added-data-files') AS archivos_agregados,
               element_at(summary, 'total-records') AS filas_totales
        FROM "{c}"."{e}"."{t}$snapshots" ORDER BY committed_at""")


def objetos_s3(nombre_tabla: str) -> dict:
    """Objetos reales en MinIO bajo la ubicación de la tabla (datos + metadatos), con s3fs."""
    loc = catalogo("main").load_table(nombre_tabla).location()
    fs = _fs()
    res = {"ubicacion": loc}
    for sub in ("data", "metadata"):
        info = fs.find(f"{loc}/{sub}".replace("s3://", ""), detail=True)
        res[sub] = {"objetos": len(info), "bytes": sum(v["size"] for v in info.values())}
    return res


def _fs():
    import s3fs

    return s3fs.S3FileSystem(key=os.environ["AWS_ACCESS_KEY_ID"], secret=os.environ["AWS_SECRET_ACCESS_KEY"],
                             client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]})


def commit_con_hora(ns, clave: str, hora: str) -> dict:
    """Recorre el historial de main y devuelve el commit más reciente en que la tabla terminaba en `hora`."""
    fs, cache = _fs(), {}
    for c in ns.historial("main", 1000):
        cont = ns.contenido(f"main@{c['hash']}", clave)
        if not cont:
            continue
        loc = cont["metadataLocation"]
        if loc not in cache:
            meta = json.loads(fs.cat(loc.replace("s3://", "")))
            snap = next(x for x in meta["snapshots"] if x["snapshot-id"] == meta["current-snapshot-id"])
            cache[loc] = {"snapshots_en_metadata": len(meta["snapshots"]), "snapshot_id": snap["snapshot-id"],
                          "hora": snap["summary"].get("lakehouse.hora")}
        if cache[loc]["hora"] == hora:
            return {"hash": c["hash"], "commitTime": c["commitTime"], "metadataLocation": loc, **cache[loc]}
    raise LookupError(hora)


def time_travel() -> None:
    from lakehouse.nessie import Nessie

    ns, con = Nessie(os.environ["NESSIE_URI"]), trino()
    objetivo = commit_con_hora(ns, "wiki.pageviews_limpio", "2026-07-18T23:00")
    ns.asignar_etiqueta("historia", objetivo["hash"])
    q = "SELECT count(*) AS filas, sum(vistas) AS vistas, min(hora) AS desde, max(hora) AS hasta FROM {t}"
    top = ("SELECT titulo, sum(vistas) AS vistas FROM {t} WHERE idioma = 'es' "
           "GROUP BY titulo ORDER BY vistas DESC LIMIT 5")
    hist = "iceberg_historia.wiki.pageviews_limpio"
    datos = {
        "commit_nessie_fin_18_jul": objetivo,
        "snapshots_en_tabla_main": uno(con, 'SELECT count(*) FROM iceberg.wiki."pageviews_limpio$snapshots"'),
        "commits_en_main": len(ns.historial("main", 1000)),
        "actual_main": filas_dict(con, q.format(t=LIMPIO))[0],
        "etiqueta_historia": filas_dict(con, q.format(t=hist))[0],
        "top5_es_actual": filas_dict(con, top.format(t=LIMPIO)),
        "top5_es_fin_18": filas_dict(con, top.format(t=hist)),
    }
    try:  # time travel nativo de Iceberg al snapshot de ese commit: con Nessie REST ya no está en la metadata
        uno(con, f"SELECT count(*) FROM {LIMPIO} FOR VERSION AS OF {objetivo['snapshot_id']}")
        datos["for_version_as_of_snapshot_viejo"] = "funciona"
    except Exception as e:
        datos["for_version_as_of_snapshot_viejo"] = str(e).split(chr(10))[0][:300]
    guardar(SALIDA / "01_time_travel.json", datos)
    print(datos["actual_main"], datos["etiqueta_historia"], datos["for_version_as_of_snapshot_viejo"])


def esquema() -> None:
    con = trino()
    antes = {"archivos": archivos(con, LIMPIO), "snapshots": len(snapshots(con, LIMPIO)),
             "columnas": [f[0] for f in ejecutar(con, f"DESCRIBE {LIMPIO}")[0]]}
    t0 = time.perf_counter()
    ejecutar(con, f"ALTER TABLE {LIMPIO} ADD COLUMN lote varchar COMMENT 'lote de carga (trazabilidad)'")
    segundos = round(time.perf_counter() - t0, 2)
    despues = {"archivos": archivos(con, LIMPIO), "snapshots": len(snapshots(con, LIMPIO)),
               "columnas": [f[0] for f in ejecutar(con, f"DESCRIBE {LIMPIO}")[0]]}
    datos = {
        "sentencia": "ALTER TABLE iceberg.wiki.pageviews_limpio ADD COLUMN lote varchar",
        "segundos": segundos, "antes": antes, "despues": despues,
        "archivos_reescritos": antes["archivos"]["bytes"] != despues["archivos"]["bytes"],
        "filas_con_lote_nulo": uno(con, f"SELECT count_if(lote IS NULL) FROM {LIMPIO}"),
        "historial_metadatos": filas_dict(con, 'SELECT timestamp, latest_schema_id, latest_sequence_number '
                                               'FROM iceberg.wiki."pageviews_limpio$metadata_log_entries" '
                                               'ORDER BY timestamp DESC LIMIT 3'),
    }
    guardar(SALIDA / "02_evolucion_esquema.json", datos)
    print(segundos, antes["archivos"], despues["archivos"])


def _particiones(con) -> list[dict]:
    return filas_dict(con, """
        SELECT spec_id, count(*) AS archivos, sum(record_count) AS filas, round(sum(file_size_in_bytes)/1e6, 1) AS mb,
               min(json_format(CAST(partition AS json))) AS ejemplo_particion
        FROM iceberg.wiki."pageviews_limpio$files" GROUP BY spec_id ORDER BY spec_id""")


def particiones_antes() -> None:
    con = trino()
    resultados = {k: filas_dict(con, q) for k, q in CONSULTAS_REFERENCIA.items()}
    ejecutar(con, f"ALTER TABLE {LIMPIO} SET PROPERTIES partitioning = ARRAY['idioma', 'hour(hora)']")
    guardar(SALIDA / "03a_particiones_antes.json", {
        "sentencia": "ALTER TABLE iceberg.wiki.pageviews_limpio SET PROPERTIES "
                     "partitioning = ARRAY['idioma', 'hour(hora)']",
        "consultas": resultados, "archivos_por_spec": _particiones(con),
        "ddl_despues": uno(con, f"SHOW CREATE TABLE {LIMPIO}")})


def particiones_despues() -> None:
    con = trino()
    antes = json.loads((SALIDA / "03a_particiones_antes.json").read_text(encoding="utf-8"))
    resultados = {k: filas_dict(con, q) for k, q in CONSULTAS_REFERENCIA.items()}
    iguales = {k: json.dumps(v, default=str) == json.dumps(antes["consultas"][k], default=str)
               for k, v in resultados.items()}
    # La misma consulta de una hora lee un día entero en la parte diaria y solo esa hora en la parte horaria
    lectura = {}
    for dia in ("2026-07-19", "2026-07-20"):
        sql = (f"SELECT titulo, sum(vistas) v FROM {LIMPIO} WHERE idioma = 'en' AND hora = TIMESTAMP '{dia} 19:00:00' "
               "GROUP BY titulo ORDER BY v DESC LIMIT 3")
        corridas = [ejecutar(con, sql)[1] for _ in range(3)]
        lectura[dia] = {"physicalInputBytes": corridas[-1]["physicalInputBytes"],
                        "processedRows": corridas[-1]["processedRows"],
                        "ms_mediana": sorted(c["elapsedTimeMillis"] for c in corridas)[1]}
    datos = {"consultas_viejas_mismo_resultado": iguales, "consultas": resultados,
             "archivos_por_spec": _particiones(con), "lectura_una_hora": lectura,
             "specs": filas_dict(con, "SELECT DISTINCT spec_id FROM iceberg.wiki.\"pageviews_limpio$files\"")}
    guardar(SALIDA / "03b_particiones_despues.json", datos)
    print(iguales, lectura)


def _estado_crudo(con) -> dict:
    return {"archivos": archivos(con, CRUDO), "minio": objetos_s3("wiki.pageviews_crudo"),
            "filas": uno(con, f"SELECT count(*) FROM {CRUDO}"),
            "vistas": uno(con, f"SELECT sum(CAST(vistas AS bigint)) FROM {CRUDO}")}


def compactacion() -> None:
    """Paso 1: optimize en Trino. (El paso 2 es nessie-gc, que se corre desde el host; luego `compactacion_final`.)"""
    from lakehouse.nessie import Nessie

    con = trino()
    ns = Nessie(os.environ["NESSIE_URI"])
    pasos = {"antes": _estado_crudo(con)}
    t0 = time.perf_counter()
    ejecutar(con, f"ALTER TABLE {CRUDO} EXECUTE optimize(file_size_threshold => '128MB')")
    pasos["despues_optimize"] = {**_estado_crudo(con), "segundos": round(time.perf_counter() - t0, 1)}
    try:
        t0 = time.perf_counter()
        ejecutar(con, f"ALTER TABLE {CRUDO} EXECUTE expire_snapshots(retention_threshold => '0s')")
        pasos["expire_snapshots_trino"] = {"resultado": "ejecutado", "segundos": round(time.perf_counter() - t0, 1),
                                           **_estado_crudo(con)}
    except Exception as e:
        pasos["expire_snapshots_trino"] = {"resultado": str(e).split(chr(10))[0][:300]}
    pasos["propiedades"] = {f["key"]: f["value"] for f in filas_dict(
        con, 'SELECT * FROM iceberg.wiki."pageviews_crudo$properties"') if f["key"].startswith(("gc", "nessie"))}
    # La etiqueta `historia` mantiene vivo lo que referencia; se mueve a la cabeza de main para que el GC pueda
    # borrar los archivos pequeños que solo usan commits viejos.
    ns.asignar_etiqueta("historia", ns.hash("main"))
    guardar(SALIDA / "04_compactacion.json", pasos)
    for k in ("antes", "despues_optimize"):
        print(k, pasos[k]["archivos"], pasos[k]["minio"]["data"])
    print(pasos["expire_snapshots_trino"])


def compactacion_final() -> None:
    con = trino()
    pasos = json.loads((SALIDA / "04_compactacion.json").read_text(encoding="utf-8"))
    pasos["despues_nessie_gc"] = _estado_crudo(con)
    guardar(SALIDA / "04_compactacion.json", pasos)
    print(pasos["despues_nessie_gc"]["archivos"], pasos["despues_nessie_gc"]["minio"]["data"])


if __name__ == "__main__":
    globals()[sys.argv[1]]()
