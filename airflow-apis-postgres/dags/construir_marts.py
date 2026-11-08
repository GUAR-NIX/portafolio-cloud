"""
### Marts para BI · se dispara por datos, no por horario

`schedule = clima | banco_mundial`: corre cuando cualquiera de los dos DAG de ingesta termina sus controles de
calidad en verde (eventos de *asset*). Si llegan varios eventos mientras corre, Airflow los agrupa en la
siguiente ejecución, así que un backfill de 380 días no genera 380 reconstrucciones.

`publicar_marts` hace **write-audit-publish en una sola transacción** `REPEATABLE READ`:
reconstruye las 7 tablas de `marts`, corre 7 controles de conciliación contra la **misma foto** de staging y
solo hace `COMMIT` si todos pasan. Si falla uno, `ROLLBACK`: los tableros siguen viendo la versión anterior.

> Primera versión: construir y validar eran dos tareas. Durante el backfill la validación fallaba porque las
> ingestas seguían escribiendo en staging entre una tarea y la otra (staging tenía 13 filas más que el mart).
"""
from __future__ import annotations

import time

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

from ingesta import bd, calidad
from ingesta.airflow_comun import ARGUMENTOS, ASSET_BANCO_MUNDIAL, ASSET_CLIMA, ASSET_MARTS, conexion


@dag(
    dag_id="construir_marts",
    description="staging → marts (7 tablas) con conciliación antes de publicar",
    schedule=(ASSET_CLIMA | ASSET_BANCO_MUNDIAL),
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Guayaquil"),
    catchup=False,
    max_active_runs=1,
    default_args=ARGUMENTOS,
    tags=["marts", "bi"],
    doc_md=__doc__,
)
def construir_marts():

    @task(outlets=[ASSET_MARTS])
    def publicar_marts(ti=None) -> dict:
        conn = conexion()
        bd.asegurar_esquemas(conn)
        resultados: list = []

        def validar(cur):
            resultados.extend(calidad.evaluar_controles(cur, calidad.controles_marts(), {}))
            calidad.registrar_resultados(cur, resultados, "marts", "completo", ti.dag_id, ti.run_id)
            calidad.exigir(resultados, "marts", "completo")

        inicio = time.monotonic()
        try:
            tablas = bd.construir_marts(conn, validar=validar)
        except calidad.ErrorCalidad as e:
            # el ROLLBACK también deshizo la auditoría: se registra aparte para que el fallo quede a la vista
            with conn.cursor() as cur:
                calidad.registrar_resultados(cur, e.resultados, "marts", "completo", ti.dag_id, ti.run_id)
            conn.commit()
            raise AirflowFailException(str(e)) from e      # un dato malo no se arregla reintentando
        segundos = time.monotonic() - inicio
        bd.registrar_carga(conn, ti.dag_id, ti.run_id, ti.task_id, "marts", "completo", sum(tablas.values()),
                           segundos, tablas)
        for tabla, filas in tablas.items():
            print(f"marts.{tabla:<24} {filas:>8,} filas")
        print(f"Construcción + {len(resultados)} controles + COMMIT en {segundos:.2f} s")
        return tablas

    publicar_marts()


construir_marts()
