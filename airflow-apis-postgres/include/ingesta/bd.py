"""Cargas a Postgres. Reciben una conexión DB-API (psycopg2), así que no dependen de Airflow:
en los DAG la conexión sale de `PostgresHook`; en las pruebas, de `psycopg2.connect`.

Idempotencia: cada partición (un día de clima, un año×indicador del Banco Mundial) se reemplaza completa
dentro de una transacción. Re-ejecutar la misma fecha deja exactamente el mismo resultado.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

from psycopg2.extras import Json, execute_values

from ingesta import config
from ingesta.transformar import filas_banco_mundial, filas_clima

RAIZ_SQL = Path(__file__).resolve().parents[2] / "sql"
CANDADO_ESQUEMAS = 4_242_001        # pg_advisory_xact_lock: evita carreras de CREATE SCHEMA entre DAG runs


def leer_sql(nombre: str) -> str:
    return (RAIZ_SQL / nombre).read_text(encoding="utf-8")


def asegurar_esquemas(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (CANDADO_ESQUEMAS,))
        cur.execute(leer_sql("00_esquemas.sql"))
        execute_values(cur, """
            INSERT INTO staging.ciudades (ciudad, provincia, region, latitud, longitud) VALUES %s
            ON CONFLICT (ciudad) DO UPDATE SET provincia = EXCLUDED.provincia, region = EXCLUDED.region,
                latitud = EXCLUDED.latitud, longitud = EXCLUDED.longitud""",
            [(c["ciudad"], c["provincia"], c["region"], c["latitud"], c["longitud"]) for c in config.CIUDADES])
    conn.commit()


def _huella(payload) -> tuple[str, int]:
    texto = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
    return hashlib.sha256(texto).hexdigest(), len(texto)


# ------------------------------------------------------------------------------------------------- clima
def raw_clima_definitivo(conn, fecha: date) -> bool:
    """¿Ya está la respuesta del archivo ERA5 para esa fecha? Entonces no hace falta volver a pedirla."""
    with conn.cursor() as cur:
        cur.execute("SELECT fuente FROM raw.clima_respuestas WHERE fecha = %s", (fecha,))
        fila = cur.fetchone()
    return bool(fila and fila[0] == "archivo")


def segundos_desde_ultima_descarga_clima(conn) -> float | None:
    """Base del limitador de ritmo: con el pool de 1 cupo, raw es el reloj compartido entre ejecuciones."""
    with conn.cursor() as cur:
        cur.execute("SELECT extract(epoch FROM clock_timestamp() - max(descargado_en)) FROM raw.clima_respuestas")
        valor = cur.fetchone()[0]
    conn.commit()
    return None if valor is None else float(valor)


def guardar_raw_clima(conn, fecha: date, descarga: dict) -> int:
    sha, n_bytes = _huella(descarga["respuesta"])
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.clima_respuestas (fecha, fuente, url, parametros, payload, sha256, bytes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fecha) DO UPDATE SET fuente = EXCLUDED.fuente, url = EXCLUDED.url,
                parametros = EXCLUDED.parametros, payload = EXCLUDED.payload, sha256 = EXCLUDED.sha256,
                bytes = EXCLUDED.bytes, descargas = raw.clima_respuestas.descargas + 1, descargado_en = now()""",
            (fecha, descarga["fuente"], descarga["url"], Json(descarga["parametros"]), Json(descarga["respuesta"]),
             sha, n_bytes))
    conn.commit()
    return n_bytes


def cargar_staging_clima(conn, fecha: date) -> int:
    """Lee el JSON de raw (no de XCom ni de memoria): staging siempre se puede reconstruir desde raw."""
    with conn.cursor() as cur:
        cur.execute("SELECT payload, fuente FROM raw.clima_respuestas WHERE fecha = %s", (fecha,))
        fila = cur.fetchone()
        if fila is None:
            raise LookupError(f"No hay respuesta cruda de clima para {fecha}")
        filas = filas_clima(fila[0], fecha, fila[1])
        columnas = list(filas[0])
        cur.execute("DELETE FROM staging.clima_diario WHERE fecha = %s", (fecha,))
        execute_values(cur, f"INSERT INTO staging.clima_diario ({', '.join(columnas)}) VALUES %s",
                       [tuple(f[c] for c in columnas) for f in filas])
    conn.commit()
    return len(filas)


# ----------------------------------------------------------------------------------------- Banco Mundial
def guardar_raw_banco_mundial(conn, anio: int, indicador: str, registros: list[dict]) -> int:
    sha, _ = _huella(registros)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.banco_mundial_respuestas (anio, indicador, payload, registros, sha256)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (anio, indicador) DO UPDATE SET payload = EXCLUDED.payload,
                registros = EXCLUDED.registros, sha256 = EXCLUDED.sha256,
                descargas = raw.banco_mundial_respuestas.descargas + 1, descargado_en = now()""",
            (anio, indicador, Json(registros), len(registros), sha))
    conn.commit()
    return len(registros)


def cargar_staging_banco_mundial(conn, anio: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT indicador, payload FROM raw.banco_mundial_respuestas WHERE anio = %s ORDER BY 1",
                    (anio,))
        crudos = cur.fetchall()
        if not crudos:
            raise LookupError(f"No hay respuestas crudas del Banco Mundial para {anio}")
        filas = [f for indicador, payload in crudos for f in filas_banco_mundial(payload, indicador, anio)]
        cur.execute("DELETE FROM staging.indicadores_bm WHERE anio = %s", (anio,))
        if filas:
            columnas = list(filas[0])
            execute_values(cur, f"INSERT INTO staging.indicadores_bm ({', '.join(columnas)}) VALUES %s",
                           [tuple(f[c] for c in columnas) for f in filas])
    conn.commit()
    return len(filas)


# ------------------------------------------------------------------------------------------------- marts
def construir_marts(conn, validar=None) -> dict[str, int]:
    """Write-audit-publish en UNA transacción REPEATABLE READ.

    Todas las sentencias ven la misma foto de staging aunque las ingestas sigan escribiendo en paralelo, así que
    la conciliación mart ↔ staging compara contra exactamente lo que se usó para construir. `validar(cursor)`
    corre antes del COMMIT: si lanza una excepción se hace ROLLBACK y los tableros siguen viendo la versión
    anterior completa (el DDL de Postgres es transaccional).
    """
    conn.rollback()
    conn.set_session(isolation_level="REPEATABLE READ")
    tablas = {}
    try:
        with conn.cursor() as cur:
            for archivo in sorted((RAIZ_SQL / "marts").glob("*.sql")):
                cur.execute(archivo.read_text(encoding="utf-8"))
            cur.execute("""SELECT table_name FROM information_schema.tables
                           WHERE table_schema = 'marts' ORDER BY 1""")
            for (tabla,) in cur.fetchall():
                cur.execute(f"SELECT count(*) FROM marts.{tabla}")
                tablas[tabla] = cur.fetchone()[0]
            if validar is not None:
                validar(cur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.set_session(isolation_level="DEFAULT")
    return tablas


def registrar_carga(conn, dag_id: str, run_id: str, tarea: str, capa: str, particion: str, filas: int,
                    segundos: float, detalle: dict | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO auditoria.cargas (dag_id, run_id, tarea, capa, particion, filas, segundos, detalle)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (dag_id, run_id, tarea, capa, particion, filas, round(segundos, 3), Json(detalle or {})))
    conn.commit()
