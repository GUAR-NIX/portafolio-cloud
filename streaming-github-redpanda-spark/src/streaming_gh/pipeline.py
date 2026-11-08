"""Pipeline de streaming: Redpanda → bronce → plata → oro (Delta Lake en MinIO), todo en un mismo driver.

Cada capa es una consulta de Structured Streaming con su propio checkpoint. Todas escriben con `foreachBatch` y
escrituras idempotentes de Delta (`txnAppId` + `txnVersion` = id del micro-lote): si el job muere después de
escribir un lote pero antes de registrarlo en el checkpoint, al reanudar Spark repite el lote y Delta lo ignora.
Así el sistema es exactamente-una-vez de punta a punta aunque se le haga `docker kill`.

    python -m streaming_gh.pipeline --corrida principal                    # corre hasta que se detenga
    python -m streaming_gh.pipeline --corrida principal --disponible-ahora  # procesa lo pendiente y termina
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener
from pyspark.sql.types import StructType

from streaming_gh.config import KAFKA_BOOTSTRAP, WATERMARK_ORO, WATERMARK_PLATA, Corrida
from streaming_gh.spark_comun import crear_sesion
from streaming_gh.transformaciones import (
    agregar_actividad,
    agregar_lenguajes,
    agregar_repos,
    bronce_desde_kafka,
    deduplicar,
    detectar_picos_actividad,
    detectar_picos_repos,
    plata_desde_bronce,
)

# Inyección de fallos: FALLA_TRAS_ESCRIBIR="plata:5" mata el proceso (os._exit, como un SIGKILL) justo después de
# escribir el lote 5 de plata en Delta y ANTES de que Spark lo confirme en el checkpoint: el peor momento posible.
FALLA_TRAS_ESCRIBIR = os.environ.get("FALLA_TRAS_ESCRIBIR", "")
VENTANAS_BASE_ACTIVIDAD = 12  # 1 hora de ventanas de 5 min
HORAS_BASE_REPOS = 24


# ---------------------------------------------------------------------------------------------------------- métricas

class Metricas:
    """Escribe JSON por línea en reports/metricas/<corrida>/: lotes.jsonl (cada micro-lote escrito, con latencia
    medida) y progreso.jsonl (el StreamingQueryProgress de Spark: tasas, duraciones, watermark, estado)."""

    def __init__(self, directorio: Path):
        self.directorio = directorio
        directorio.mkdir(parents=True, exist_ok=True)
        self._candado = threading.Lock()

    def escribir(self, archivo: str, datos: dict) -> None:
        linea = json.dumps(datos, ensure_ascii=False, default=str)
        with self._candado, open(self.directorio / archivo, "a", encoding="utf-8") as f:
            f.write(linea + "\n")


class Oyente(StreamingQueryListener):
    def __init__(self, metricas: Metricas):
        self.metricas = metricas

    def onQueryStarted(self, event):
        self.metricas.escribir("eventos.jsonl", {"evento": "consulta_iniciada", "consulta": event.name,
                                                 "ts": datetime.now(UTC).isoformat()})

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        self.metricas.escribir("progreso.jsonl", {
            "consulta": p.get("name"),
            "lote": p.get("batchId"),
            "ts": p.get("timestamp"),
            "filas_entrada": p.get("numInputRows"),
            "eps_entrada": p.get("inputRowsPerSecond"),
            "eps_procesado": p.get("processedRowsPerSecond"),
            "duracion_ms": p.get("durationMs", {}),
            "watermark": (p.get("eventTime") or {}).get("watermark"),
            "estado": [{"filas": s.get("numRowsTotal"), "descartadas_watermark": s.get("numRowsDroppedByWatermark"),
                        "memoria_bytes": s.get("memoryUsedBytes")} for s in p.get("stateOperators", [])],
        })

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        self.metricas.escribir("eventos.jsonl", {"evento": "consulta_terminada", "id": str(event.id),
                                                 "error": event.exception, "ts": datetime.now(UTC).isoformat()})


def latencias(df: DataFrame, t_fin: float, columna: str) -> dict:
    """Latencia de extremo a extremo = momento en que el lote quedó escrito en Delta − timestamp de Kafka (hora en
    que el productor publicó el mensaje). p95 de la latencia = t_fin − percentil 5 del timestamp."""
    fila = df.agg(
        F.percentile_approx(F.col(columna).cast("double"), [0.05, 0.5], 1000).alias("p"),
        F.min(F.col(columna)).cast("double").alias("minimo"),
    ).first()
    if fila is None or fila["p"] is None:
        return {}
    p05, p50 = fila["p"]
    return {"lat_p50_s": round(t_fin - p50, 3), "lat_p95_s": round(t_fin - p05, 3),
            "lat_max_s": round(t_fin - fila["minimo"], 3)}


# ----------------------------------------------------------------------------------------------------------- Delta

def escribir_idempotente(df: DataFrame, ruta: str, app_id: str, lote: int, particiones: list[str] | None = None):
    escritor = (df.write.format("delta").mode("append")
                .option("txnAppId", app_id).option("txnVersion", lote))
    if particiones:
        escritor = escritor.partitionBy(*particiones)
    escritor.save(ruta)


def version_delta(spark: SparkSession, ruta: str) -> int | None:
    """Última versión del log de Delta (vía la JVM: lista el log sin lanzar un job de Spark)."""
    try:
        log = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, ruta)
        # update(stalenessAcceptable, checkIfUpdatedSinceTs): Py4J no ve los valores por defecto de Scala
        return log.update(False, spark._jvm.scala.Option.empty()).version()
    except Exception:  # noqa: BLE001 - la tabla todavía no existe
        return None


def asegurar_tabla(spark: SparkSession, esquema: StructType, ruta: str, particiones: list[str] | None = None):
    """Crea la tabla vacía si no existe, para que la capa siguiente pueda leerla en streaming desde el arranque."""
    if DeltaTable.isDeltaTable(spark, ruta):
        return
    escritor = spark.createDataFrame([], esquema).write.format("delta").mode("ignore")
    if particiones:
        escritor = escritor.partitionBy(*particiones)
    escritor.save(ruta)


def existe_tabla(spark: SparkSession, ruta: str) -> bool:
    return DeltaTable.isDeltaTable(spark, ruta)


# ------------------------------------------------------------------------------------------------------- consultas

def _procesador(capa: str, ruta: str, metricas: Metricas, columna_latencia: str,
                particiones: list[str] | None = None,
                despues: Callable[[DataFrame, int], dict] | None = None) -> Callable[[DataFrame, int], None]:
    """foreachBatch común: escribir idempotente + medir latencia + (opcional) un paso extra como los picos."""

    def procesar(df: DataFrame, lote: int) -> None:
        t_inicio = time.time()
        df = df.persist()
        try:
            filas = df.count()
            extra = {}
            if filas:
                v0 = version_delta(df.sparkSession, ruta)
                escribir_idempotente(df, ruta, capa, lote, particiones)
                if v0 is not None and version_delta(df.sparkSession, ruta) == v0:
                    # el lote ya estaba escrito (el job murió antes de confirmarlo en el checkpoint)
                    extra["escritura_omitida_por_txn"] = True
                if f"{capa}:{lote}" == FALLA_TRAS_ESCRIBIR and "escritura_omitida_por_txn" not in extra:
                    metricas.escribir("eventos.jsonl", {"evento": "falla_inyectada", "capa": capa, "lote": lote,
                                                        "filas": filas, "ts": datetime.now(UTC).isoformat()})
                    os._exit(137)
            t_escrito = time.time()
            if filas:
                extra |= latencias(df, t_escrito, columna_latencia)
                if despues:
                    extra |= despues(df, lote)
            metricas.escribir("lotes.jsonl", {
                "capa": capa, "lote": lote, "filas": filas, "t_inicio": round(t_inicio, 3),
                "t_escrito": round(t_escrito, 3), "duracion_s": round(t_escrito - t_inicio, 3), **extra,
            })
        finally:
            df.unpersist()

    return procesar


def _iniciar(df: DataFrame, nombre: str, checkpoint: str, procesar, trigger: dict,
             modo: str = "append") -> StreamingQuery:
    return (df.writeStream.queryName(nombre).outputMode(modo).foreachBatch(procesar)
            .option("checkpointLocation", checkpoint).trigger(**trigger).start())


def iniciar_bronce(spark: SparkSession, corrida: Corrida, metricas: Metricas, trigger: dict,
                   max_offsets: int = 300_000) -> StreamingQuery:
    from streaming_gh.productor import crear_tema

    crear_tema(corrida.tema)  # si el pipeline arranca antes que el productor, el tema ya existe y espera datos
    fuente = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", corrida.tema)
        .option("startingOffsets", "earliest")
        .option("includeHeaders", "true")
        .option("maxOffsetsPerTrigger", max_offsets)  # lotes acotados al ponerse al día tras una caída
        .load()
    )
    df = bronce_desde_kafka(fuente)
    ruta = corrida.tabla("bronce", "eventos")
    asegurar_tabla(spark, df.schema, ruta, ["fecha_evento"])
    procesar = _procesador("bronce", ruta, metricas, "ts_kafka", ["fecha_evento"])
    return _iniciar(df, "bronce", corrida.checkpoint("bronce"), procesar, trigger)


def iniciar_plata(spark: SparkSession, corrida: Corrida, metricas: Metricas, trigger: dict,
                  opciones_lectura: dict | None = None) -> StreamingQuery:
    fuente = spark.readStream.format("delta").options(**(opciones_lectura or {})).load(
        corrida.tabla("bronce", "eventos"))
    df = deduplicar(plata_desde_bronce(fuente), WATERMARK_PLATA)
    ruta = corrida.tabla("plata", "eventos")
    asegurar_tabla(spark, df.schema, ruta, ["fecha"])
    procesar = _procesador("plata", ruta, metricas, "ts_kafka", ["fecha"])
    return _iniciar(df, "plata", corrida.checkpoint("plata"), procesar, trigger)


def iniciar_oro(spark: SparkSession, corrida: Corrida, metricas: Metricas, trigger: dict,
                opciones_lectura: dict | None = None) -> list[StreamingQuery]:
    ruta_plata = corrida.tabla("plata", "eventos")
    ruta_act = corrida.tabla("oro", "actividad_5min")
    ruta_repos = corrida.tabla("oro", "repos_hora")
    ruta_picos_act = corrida.tabla("oro", "picos_actividad")
    ruta_picos_repos = corrida.tabla("oro", "picos_repos")

    def picos_actividad(df: DataFrame, lote: int) -> dict:
        minimo = df.agg(F.min("ventana_inicio")).first()[0]
        historia = (spark.read.format("delta").load(ruta_act)
                    .where((F.col("ventana_inicio") < F.lit(minimo))
                           & (F.col("ventana_inicio") >= F.lit(minimo)
                              - F.expr(f"INTERVAL {5 * (VENTANAS_BASE_ACTIVIDAD + 1)} MINUTES"))))
        picos = detectar_picos_actividad(df, historia, VENTANAS_BASE_ACTIVIDAD).persist()
        n = picos.count()
        if n:
            escribir_idempotente(picos, ruta_picos_act, "picos_actividad", lote)
        picos.unpersist()
        return {"picos": n}

    def picos_repos(df: DataFrame, lote: int) -> dict:
        candidatos = df.where(F.col("estrellas") >= 15)
        if candidatos.isEmpty():
            return {"picos": 0}
        tabla = spark.read.format("delta").load(ruta_repos)
        primera = tabla.agg(F.min("ventana_inicio")).first()[0]
        minimo = candidatos.agg(F.min("ventana_inicio")).first()[0]
        # El lote ya está escrito en la tabla, así que la historia incluye sus ventanas anteriores; el join de
        # detectar_picos_repos solo toma ventanas estrictamente previas a la de cada candidato.
        historia = (tabla.where(F.col("ventana_inicio") >= F.lit(minimo)
                                - F.expr(f"INTERVAL {HORAS_BASE_REPOS} HOURS"))
                    .join(candidatos.select("repo_id").distinct(), "repo_id", "left_semi"))
        picos = detectar_picos_repos(candidatos, historia, primera, HORAS_BASE_REPOS).persist()
        n = picos.count()
        if n:
            escribir_idempotente(picos, ruta_picos_repos, "picos_repos", lote)
        picos.unpersist()
        return {"picos": n}

    def fuente() -> DataFrame:  # una lectura por consulta: cada una lleva su propio avance en el checkpoint
        return spark.readStream.format("delta").options(**(opciones_lectura or {})).load(ruta_plata)

    consultas = []
    for nombre, agregado, ruta, despues in [
        ("actividad_5min", agregar_actividad, ruta_act, picos_actividad),
        ("repos_hora", agregar_repos, ruta_repos, picos_repos),
        ("lenguajes_hora", agregar_lenguajes, corrida.tabla("oro", "lenguajes_hora"), None),
    ]:
        df = agregado(fuente(), WATERMARK_ORO)
        procesar = _procesador(f"oro_{nombre}", ruta, metricas, "ts_kafka_max", None, despues)
        consultas.append(_iniciar(df, f"oro_{nombre}", corrida.checkpoint(f"oro_{nombre}"), procesar, trigger))
    return consultas


# ------------------------------------------------------------------------------------------------------------ main

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--corrida", required=True)
    parser.add_argument("--capas", default="bronce,plata,oro")
    parser.add_argument("--trigger-s", type=float, default=3.0, help="intervalo de micro-lote de bronce y plata")
    parser.add_argument("--trigger-oro-s", type=float, default=10.0)
    parser.add_argument("--max-offsets", type=int, default=300_000)
    parser.add_argument("--disponible-ahora", action="store_true",
                        help="procesa lo pendiente capa por capa (trigger availableNow) y termina")
    args = parser.parse_args()

    corrida = Corrida(args.corrida)
    capas = args.capas.split(",")
    spark = crear_sesion(f"streaming-gh-{corrida.nombre}")
    metricas = Metricas(corrida.dir_metricas)
    spark.streams.addListener(Oyente(metricas))
    metricas.escribir("eventos.jsonl", {"evento": "arranque", "ts": datetime.now(UTC).isoformat(), "args": vars(args)})

    def trigger(segundos: float) -> dict:
        return {"availableNow": True} if args.disponible_ahora else {"processingTime": f"{segundos:g} seconds"}

    inicios = []
    if "bronce" in capas:
        inicios.append(lambda: [iniciar_bronce(spark, corrida, metricas, trigger(args.trigger_s), args.max_offsets)])
    if "plata" in capas:
        inicios.append(lambda: [iniciar_plata(spark, corrida, metricas, trigger(args.trigger_s))])
    if "oro" in capas:
        inicios.append(lambda: iniciar_oro(spark, corrida, metricas, trigger(args.trigger_oro_s)))

    def detener(signum, _frame):
        print(f"Señal {signum}: deteniendo consultas", flush=True)
        for q in spark.streams.active:
            q.stop()

    signal.signal(signal.SIGTERM, detener)
    signal.signal(signal.SIGINT, detener)

    if args.disponible_ahora:  # cada capa ve lo que dejó la anterior
        for inicio in inicios:
            for q in inicio():
                q.awaitTermination()
    else:
        for inicio in inicios:
            inicio()
        print(f"Corrida {corrida.nombre}: {len(spark.streams.active)} consultas activas", flush=True)
        while spark.streams.active:
            spark.streams.awaitAnyTermination()
            spark.streams.resetTerminated()
    metricas.escribir("eventos.jsonl", {"evento": "fin", "ts": datetime.now(UTC).isoformat()})
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
