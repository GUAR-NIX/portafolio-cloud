"""
### Ingesta diaria de clima · 13 ciudades del Ecuador (Open-Meteo)

Una ejecución = **un día** (`data_interval_start` en hora de Ecuador). Con `catchup=True`, al activar el DAG
Airflow crea una ejecución por cada día desde `start_date`: así se hace el backfill.

1. `extraer_a_raw`: una petición con las 13 ciudades → `raw.clima_respuestas` (JSON completo).
   Días con más de 5 días de antigüedad van al archivo ERA5 (definitivo); los recientes, a la API de
   pronóstico (provisional, se reemplaza al re-ejecutar). Si raw ya tiene el dato definitivo, no se vuelve a
   pedir (re-ejecutar no gasta cuota). Pool `api_open_meteo` de 1 cupo + ritmo mínimo de 10 s entre llamadas.
2. `cargar_staging`: JSON de raw → `staging.clima_diario` (borra e inserta la fecha en una transacción).
3. `controles_calidad`: 9 controles en SQL; un crítico fuera de rango **falla el DAG** y no se emite el asset.
"""
from __future__ import annotations

import time

import pendulum
from airflow.sdk import CronDataIntervalTimetable, Param, dag, task

from ingesta import apis, bd, calidad, config
from ingesta.airflow_comun import ARGUMENTOS, ASSET_CLIMA, conexion, fecha_local, hoy_local


@dag(
    dag_id="ingesta_clima_ecuador",
    description="Open-Meteo → raw → staging, un día por ejecución",
    schedule=CronDataIntervalTimetable("0 6 * * *", timezone=config.ZONA_HORARIA),
    start_date=pendulum.datetime(config.INICIO_CLIMA.year, config.INICIO_CLIMA.month, config.INICIO_CLIMA.day,
                                 6, tz=config.ZONA_HORARIA),
    catchup=True,
    max_active_runs=4,
    dagrun_timeout=pendulum.duration(hours=1),
    default_args=ARGUMENTOS,
    params={"forzar_descarga": Param(False, type="boolean",
                                     description="Volver a pedir el día aunque raw ya tenga el dato definitivo")},
    tags=["ingesta", "clima", "open-meteo"],
    doc_md=__doc__,
)
def ingesta_clima_ecuador():

    @task(pool="api_open_meteo")
    def extraer_a_raw(data_interval_start=None, params=None, ti=None) -> dict:
        fecha = fecha_local(data_interval_start)
        conn = conexion()
        bd.asegurar_esquemas(conn)
        if bd.raw_clima_definitivo(conn, fecha) and not params["forzar_descarga"]:
            print(f"{fecha}: raw ya tiene el dato definitivo (ERA5); no se llama a la API")
            return {"fecha": fecha.isoformat(), "descargado": False}

        # Cuota gratuita de Open-Meteo: 5.000 llamadas/hora y cada ciudad cuenta como una llamada. El pool de
        # 1 cupo serializa las peticiones y aquí se espera lo que falte para el ritmo mínimo entre llamadas.
        transcurrido = bd.segundos_desde_ultima_descarga_clima(conn)
        if transcurrido is not None and transcurrido < config.SEGUNDOS_ENTRE_LLAMADAS:
            time.sleep(config.SEGUNDOS_ENTRE_LLAMADAS - transcurrido)

        inicio = time.monotonic()
        descarga = apis.descargar_clima(fecha, hoy=hoy_local())
        n_bytes = bd.guardar_raw_clima(conn, fecha, descarga)
        segundos = time.monotonic() - inicio
        bd.registrar_carga(conn, ti.dag_id, ti.run_id, ti.task_id, "raw", fecha.isoformat(), len(config.CIUDADES),
                           segundos, {"fuente": descarga["fuente"], "bytes": n_bytes})
        print(f"{fecha}: {descarga['fuente']} · {n_bytes:,} bytes · {segundos:.2f} s")
        return {"fecha": fecha.isoformat(), "descargado": True, "fuente": descarga["fuente"]}

    @task
    def cargar_staging(info: dict, ti=None) -> str:
        fecha = pendulum.parse(info["fecha"]).date()
        conn = conexion()
        inicio = time.monotonic()
        filas = bd.cargar_staging_clima(conn, fecha)
        bd.registrar_carga(conn, ti.dag_id, ti.run_id, ti.task_id, "staging", info["fecha"], filas,
                           time.monotonic() - inicio)
        print(f"{fecha}: {filas} filas en staging.clima_diario")
        return info["fecha"]

    # Sin reintentos: un control de datos no mejora por volver a correrlo.
    @task(retries=0, outlets=[ASSET_CLIMA])
    def controles_calidad(fecha: str, ti=None) -> dict:
        conn = conexion()
        try:
            with conn.cursor() as cur:
                resultados = calidad.ejecutar_controles(cur, calidad.controles_clima(), {"fecha": fecha},
                                                        "staging.clima_diario", fecha, ti.dag_id, ti.run_id)
            conn.commit()
        except calidad.ErrorCalidad:
            conn.commit()          # se guardan también los resultados fallidos en auditoría
            raise
        return {"fecha": fecha, "controles": len(resultados),
                "advertencias": [r.control.nombre for r in resultados if not r.ok]}

    controles_calidad(cargar_staging(extraer_a_raw()))


ingesta_clima_ecuador()
