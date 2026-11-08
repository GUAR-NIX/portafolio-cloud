"""
### Ingesta anual de indicadores del Banco Mundial · Ecuador y 7 países de la región

Una ejecución = **un año** de datos (2000 → último año cerrado), con `catchup=True`.

1. `esperar_api` (sensor en modo *reschedule*): espera a que la API responda sin ocupar un cupo de trabajo.
2. `extraer_indicador` con **mapeo dinámico**: una tarea por indicador (10), en el pool `api_banco_mundial`.
   Cada una guarda su JSON en `raw.banco_mundial_respuestas`; si falla un indicador, solo ese se reintenta.
3. `cargar_staging`: raw → `staging.indicadores_bm` (reemplaza el año completo).
4. `controles_calidad`: unicidad, países e indicadores esperados y rangos por indicador.

El Banco Mundial revisa cifras pasadas: para refrescar un año basta con *Clear* en esa ejecución.
"""
from __future__ import annotations

import time

import pendulum
from airflow.sdk import CronDataIntervalTimetable, PokeReturnValue, dag, task

from ingesta import apis, bd, calidad, config
from ingesta.airflow_comun import ARGUMENTOS, ASSET_BANCO_MUNDIAL, conexion, fecha_local


def anio_de(data_interval_start) -> int:
    return fecha_local(data_interval_start).year


@dag(
    dag_id="ingesta_banco_mundial",
    description="API del Banco Mundial → raw → staging, un año por ejecución",
    schedule=CronDataIntervalTimetable("0 7 1 1 *", timezone=config.ZONA_HORARIA),
    start_date=pendulum.datetime(config.INICIO_BANCO_MUNDIAL, 1, 1, 7, tz=config.ZONA_HORARIA),
    catchup=True,
    max_active_runs=3,
    dagrun_timeout=pendulum.duration(hours=1),
    default_args=ARGUMENTOS,
    tags=["ingesta", "banco-mundial", "indicadores"],
    doc_md=__doc__,
)
def ingesta_banco_mundial():

    @task.sensor(poke_interval=60, timeout=30 * 60, mode="reschedule")
    def esperar_api() -> PokeReturnValue:
        return PokeReturnValue(is_done=apis.api_banco_mundial_disponible())

    @task(pool="api_banco_mundial")
    def extraer_indicador(indicador: str, data_interval_start=None, ti=None) -> int:
        anio = anio_de(data_interval_start)
        conn = conexion()
        bd.asegurar_esquemas(conn)
        inicio = time.monotonic()
        registros = apis.descargar_indicador(indicador, anio)
        n = bd.guardar_raw_banco_mundial(conn, anio, indicador, registros)
        bd.registrar_carga(conn, ti.dag_id, ti.run_id, f"{ti.task_id}[{indicador}]", "raw", str(anio), n,
                           time.monotonic() - inicio, {"indicador": indicador})
        print(f"{anio} · {indicador}: {n} registros")
        return n

    @task
    def cargar_staging(registros: list[int], data_interval_start=None, ti=None) -> int:
        anio = anio_de(data_interval_start)
        conn = conexion()
        inicio = time.monotonic()
        filas = bd.cargar_staging_banco_mundial(conn, anio)
        bd.registrar_carga(conn, ti.dag_id, ti.run_id, ti.task_id, "staging", str(anio), filas,
                           time.monotonic() - inicio, {"registros_raw": sum(registros)})
        print(f"{anio}: {sum(registros)} registros crudos → {filas} valores no nulos en staging")
        return anio

    @task(retries=0, outlets=[ASSET_BANCO_MUNDIAL])
    def controles_calidad(anio: int, ti=None) -> dict:
        conn = conexion()
        try:
            with conn.cursor() as cur:
                resultados = calidad.ejecutar_controles(
                    cur, calidad.controles_banco_mundial(), calidad.parametros_banco_mundial(anio),
                    "staging.indicadores_bm", str(anio), ti.dag_id, ti.run_id)
            conn.commit()
        except calidad.ErrorCalidad:
            conn.commit()
            raise
        return {"anio": anio, "controles": len(resultados),
                "advertencias": [r.control.nombre for r in resultados if not r.ok]}

    conteos = extraer_indicador.expand(indicador=list(config.INDICADORES))
    esperar_api() >> conteos
    controles_calidad(cargar_staging(conteos))


ingesta_banco_mundial()
