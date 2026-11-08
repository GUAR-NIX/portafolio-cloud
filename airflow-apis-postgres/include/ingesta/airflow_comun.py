"""Piezas comunes de los DAG (sí importa Airflow; el resto del paquete `ingesta` no)."""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pendulum
import psycopg2
from airflow.sdk import Asset, Connection

from ingesta import config

log = logging.getLogger("ingesta.alertas")

# Assets: así se encadenan los DAG. Cuando las tareas de calidad terminan bien, emiten un evento sobre su
# asset y `construir_marts` arranca solo (no hay sensores consultando cada minuto ni horarios acoplados).
# Se identifican por nombre y no por URI (`postgres://...`): con URI, Airflow 3.3 inicializa los normalizadores
# de URI de TODOS los proveedores (importa los hooks de S3 y GCS) y cada tarea tardaba ~9 s extra en parsear.
ASSET_CLIMA = Asset(name="datos.staging.clima_diario")
ASSET_BANCO_MUNDIAL = Asset(name="datos.staging.indicadores_bm")
ASSET_MARTS = Asset(name="datos.marts")


def alertar_fallo(context) -> None:
    """Callback de fallo: en local deja una línea fácil de buscar en el log; en la nube aquí iría SNS/Slack."""
    ti = context["ti"]
    log.error("ALERTA · DAG %s · tarea %s · run %s · intento %s · %s", ti.dag_id, ti.task_id, ti.run_id,
              ti.try_number, context.get("exception"))


ARGUMENTOS = {
    "owner": "datos",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,     # 30 s, 1 min, 2 min…
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": alertar_fallo,
}


def conexion():
    """Conexión psycopg2 a partir de la conexión de Airflow (credenciales fuera del código).

    No se usa `PostgresHook.get_conn()` porque, según la versión del proveedor, devuelve psycopg 3, y las
    cargas usan `psycopg2.extras.execute_values`.
    """
    c = Connection.get(config.CONEXION_DATOS)
    return psycopg2.connect(host=c.host, port=c.port or 5432, dbname=c.schema, user=c.login, password=c.password,
                            application_name="airflow-ingesta")


def fecha_local(instante) -> date:
    """Inicio del intervalo de datos en hora de Ecuador -> fecha de la partición."""
    return pendulum.instance(instante).in_timezone(config.ZONA_HORARIA).date()


def hoy_local() -> date:
    return pendulum.now(config.ZONA_HORARIA).date()
