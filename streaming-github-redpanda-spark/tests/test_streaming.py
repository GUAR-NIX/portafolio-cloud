"""Comportamiento en streaming de verdad (Delta en una carpeta temporal, trigger availableNow, varios micro-lotes):
watermark y eventos tardíos, deduplicación entre lotes, el pipeline plata→oro completo y la escritura idempotente."""

import json
from datetime import datetime, timedelta

import pytest

from conftest import ESQUEMA_KAFKA, filas_kafka

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from streaming_gh.config import Corrida  # noqa: E402
from streaming_gh.pipeline import Metricas, escribir_idempotente, iniciar_oro, iniciar_plata  # noqa: E402
from streaming_gh.transformaciones import agregar_actividad, bronce_desde_kafka, deduplicar  # noqa: E402

ESQUEMA_PLATA_MIN = "id long, tipo string, creado_en timestamp, actor_id long, repo_id long, ts_kafka timestamp"


def _escribir_lotes(spark, ruta, lotes: list[list[tuple]], esquema: str):
    """Un commit (y un archivo) por lote: con maxFilesPerTrigger=1 cada uno es un micro-lote distinto."""
    for filas in lotes:
        spark.createDataFrame(filas, esquema).coalesce(1).write.format("delta").mode("append").save(ruta)


def _evento(i: int, hhmm: str, tipo: str = "PushEvent") -> tuple:
    ts = datetime.fromisoformat(f"2025-09-16 {hhmm}:00")
    return (i, tipo, ts, i, i, datetime(2025, 1, 1))


def test_watermark_descarta_los_tardios_fuera_y_acepta_los_de_dentro(spark, tmp_path):
    ruta = str(tmp_path / "plata")
    # Desde Spark 3.4 un micro-lote descarta los tardíos con el watermark del lote ANTERIOR (y desaloja estado con
    # el actual): por eso el tardío llega dos lotes después de que el watermark pasó su ventana.
    _escribir_lotes(spark, ruta, [
        [_evento(1, "10:01"), _evento(2, "10:01"), _evento(3, "10:01"), _evento(4, "10:12"), _evento(5, "10:12")],
        [_evento(6, "11:00")],                        # watermark → 11:00 − 10 min = 10:50
        [_evento(7, "11:01")],                        # watermark → 10:51; el de "tardíos" pasa a 10:50
        [_evento(8, "10:02"), _evento(9, "10:56")],   # 10:02 < 10:50: se descarta; 10:56 aún entra
        [_evento(10, "12:00")],                       # watermark → 11:50
        [_evento(11, "12:01")],
        [_evento(12, "12:02")],                       # ya se emitieron todas las ventanas hasta 11:50
    ], ESQUEMA_PLATA_MIN)
    salida = str(tmp_path / "salida")
    q = (agregar_actividad(spark.readStream.format("delta").option("maxFilesPerTrigger", 1).load(ruta), "10 minutes")
         .writeStream.format("delta").outputMode("append")
         .option("checkpointLocation", str(tmp_path / "chk")).trigger(availableNow=True).start(salida))
    q.awaitTermination()
    filas = {r["ventana_inicio"].strftime("%H:%M"): r["eventos"]
             for r in spark.read.format("delta").load(salida).collect()}
    assert filas == {"10:00": 3, "10:10": 2, "10:55": 1, "11:00": 2}  # el evento 8 (10:02) se descartó
    descartadas = sum(s.numRowsDroppedByWatermark for p in q.recentProgress for s in p.stateOperators) \
        if hasattr(q.recentProgress[0], "stateOperators") else None
    if descartadas is not None:
        assert descartadas == 1


def test_deduplicacion_entre_micro_lotes(spark, tmp_path, eventos_reducidos):
    ruta = str(tmp_path / "bronce")
    base = eventos_reducidos[:200]
    # el lote 2 repite 40 eventos del lote 1 (reenvíos del productor) y trae 100 nuevos
    lote1 = bronce_desde_kafka(spark.createDataFrame(filas_kafka(base[:100]), ESQUEMA_KAFKA))
    lote2 = bronce_desde_kafka(spark.createDataFrame(filas_kafka(base[60:200], desde_offset=100), ESQUEMA_KAFKA))
    for df in (lote1, lote2):
        df.coalesce(1).write.format("delta").mode("append").save(ruta)
    from streaming_gh.transformaciones import plata_desde_bronce

    salida = str(tmp_path / "plata")
    q = (deduplicar(plata_desde_bronce(spark.readStream.format("delta").option("maxFilesPerTrigger", 1).load(ruta)),
                    "2 hours")
         .writeStream.format("delta").option("checkpointLocation", str(tmp_path / "chk"))
         .trigger(availableNow=True).start(salida))
    q.awaitTermination()
    assert len(q.recentProgress) >= 2
    plata = spark.read.format("delta").load(salida)
    assert plata.count() == plata.select("id").distinct().count() == 200


def test_escritura_idempotente_ignora_un_lote_repetido(spark, tmp_path):
    ruta = str(tmp_path / "tabla")
    df = spark.range(10)
    escribir_idempotente(df, ruta, "capa_x", 0)
    escribir_idempotente(df, ruta, "capa_x", 1)
    escribir_idempotente(df, ruta, "capa_x", 1)  # el job murió tras escribir el lote 1 y lo repite al reanudar
    escribir_idempotente(df, ruta, "capa_x", 0)  # una versión vieja tampoco se reescribe
    assert spark.read.format("delta").load(ruta).count() == 20
    escribir_idempotente(df, ruta, "otra_capa", 1)  # cada consulta tiene su propio txnAppId
    assert spark.read.format("delta").load(ruta).count() == 30


def test_pipeline_plata_y_oro_de_punta_a_punta(spark, tmp_path, eventos_reducidos):
    corrida = Corrida("prueba", raiz=str(tmp_path / "lake"), dir_metricas_override=tmp_path / "metricas")
    ruta_bronce = corrida.tabla("bronce", "eventos")
    ordenados = sorted(eventos_reducidos, key=lambda e: e["created_at"])
    # la muestra cubre una hora; se parte en 4 commits consecutivos en el tiempo + 40 reenvíos + un evento 3 h
    # después que hace avanzar el watermark y cierra todas las ventanas
    cuarto = len(ordenados) // 4 + 1
    partes = [ordenados[i:i + cuarto] for i in range(0, len(ordenados), cuarto)]
    pr = next(e for e in reversed(ordenados) if e["payload"].get("language"))  # el watermark de lenguajes solo
    cierre = dict(pr, id="1", created_at=(datetime.fromisoformat(pr["created_at"][:19])  # avanza con eventos de PR
                                          + timedelta(hours=3)).isoformat() + "Z")
    lotes = partes + [ordenados[:40], [cierre]]
    offset = 0
    for lote in lotes:
        cab = {i: [("reenvio", b"1")] for i in range(len(lote))} if lote is lotes[-2] else None
        df = bronce_desde_kafka(spark.createDataFrame(filas_kafka(lote, cab, desde_offset=offset), ESQUEMA_KAFKA))
        df.coalesce(1).write.format("delta").mode("append").partitionBy("fecha_evento").save(ruta_bronce)
        offset += len(lote)

    metricas = Metricas(corrida.dir_metricas)
    disponible = {"availableNow": True}
    iniciar_plata(spark, corrida, metricas, disponible, {"maxFilesPerTrigger": 1}).awaitTermination()
    for q in iniciar_oro(spark, corrida, metricas, disponible, {"maxFilesPerTrigger": 1}):
        q.awaitTermination()

    plata = spark.read.format("delta").load(corrida.tabla("plata", "eventos"))
    n = len(eventos_reducidos) + 1
    assert plata.count() == plata.select("id").distinct().count() == n
    assert plata.where("es_reenvio").count() == 0  # todos los reenvíos eran duplicados

    actividad = spark.read.format("delta").load(corrida.tabla("oro", "actividad_5min"))
    repos = spark.read.format("delta").load(corrida.tabla("oro", "repos_hora"))
    # todas las ventanas de la hora de la muestra se cerraron gracias al evento de cierre
    assert actividad.agg(F.sum("eventos")).first()[0] == len(eventos_reducidos)
    assert repos.agg(F.sum("estrellas")).first()[0] == sum(1 for e in eventos_reducidos if e["type"] == "WatchEvent")
    assert spark.read.format("delta").load(corrida.tabla("oro", "lenguajes_hora")).count() > 0

    lineas = [json.loads(x) for x in (corrida.dir_metricas / "lotes.jsonl").read_text().splitlines()]
    capas = {x["capa"] for x in lineas}
    assert {"plata", "oro_actividad_5min", "oro_repos_hora", "oro_lenguajes_hora"} <= capas
    assert all("lat_p50_s" in x for x in lineas if x["filas"])
