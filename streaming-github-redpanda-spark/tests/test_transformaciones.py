"""Bronce, plata y oro en lote con Spark local, sobre eventos reales de la muestra."""

from collections import Counter
from datetime import datetime

import pytest

from conftest import ESQUEMA_KAFKA, filas_kafka

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from streaming_gh.transformaciones import (  # noqa: E402
    agregar_actividad,
    agregar_lenguajes,
    agregar_repos,
    bronce_desde_kafka,
    deduplicar,
    detectar_picos_actividad,
    detectar_picos_repos,
    plata_desde_bronce,
)


@pytest.fixture(scope="module")
def bronce(spark, eventos_reducidos):
    cabeceras = {0: [("reenvio", b"1")], 1: [("retraso_s", b"300")]}
    filas = filas_kafka(eventos_reducidos, cabeceras)
    filas.append((b"x", b"{esto no es json", "gh-eventos-prueba", 0, 99_999, filas[0][5], None))
    filas.append((b"x", b'{"type": "PushEvent"}', "gh-eventos-prueba", 0, 99_998, filas[0][5], None))  # sin id
    return bronce_desde_kafka(spark.createDataFrame(filas, ESQUEMA_KAFKA)).cache()


@pytest.fixture(scope="module")
def plata(bronce):
    return plata_desde_bronce(bronce).cache()


def test_bronce_conserva_el_mensaje_y_los_metadatos(bronce, eventos_reducidos):
    assert bronce.count() == len(eventos_reducidos) + 2
    fila = bronce.where("offset = 0").first()
    assert fila["cabeceras"] == {"reenvio": "1"}
    assert fila["fecha_evento"].isoformat() == eventos_reducidos[0]["created_at"][:10]
    assert '"type"' in fila["valor"]
    assert bronce.where("offset = 5").first()["cabeceras"] is None


def test_plata_tipa_y_descarta_invalidos(plata, eventos_reducidos):
    assert plata.count() == len(eventos_reducidos)  # los 2 mensajes inválidos no pasan
    tipos = dict(plata.dtypes)
    assert tipos["id"] == "bigint" and tipos["creado_en"] == "timestamp" and tipos["fecha"] == "date"
    e0 = eventos_reducidos[0]
    fila = plata.where(F.col("id") == int(e0["id"])).first()
    assert fila["tipo"] == e0["type"]
    assert fila["repo_nombre"] == e0["repo"]["name"]
    assert fila["creado_en"] == datetime.fromisoformat(e0["created_at"].replace("Z", ""))
    assert fila["es_reenvio"] is True
    assert plata.where(F.col("offset") == 1).first()["retraso_inyectado_s"] == 300
    assert plata.where("es_reenvio").count() == 1


def test_plata_conserva_payload_util(plata, eventos_reducidos):
    lenguajes = plata.where("lenguaje is not null").count()
    esperado = sum(1 for e in eventos_reducidos if e["payload"].get("language"))
    assert lenguajes == esperado > 0
    commits = plata.where("tipo = 'PushEvent'").agg(F.sum("commits")).first()[0]
    assert commits == sum(e["payload"]["size"] for e in eventos_reducidos if e["type"] == "PushEvent")


def test_deduplicar_por_id(spark, eventos_reducidos):
    duplicados = eventos_reducidos[:50]
    df = plata_desde_bronce(bronce_desde_kafka(
        spark.createDataFrame(filas_kafka(eventos_reducidos + duplicados), ESQUEMA_KAFKA)))
    assert df.count() == len(eventos_reducidos) + 50
    dedup = deduplicar(df, "2 hours")
    assert dedup.count() == dedup.select("id").distinct().count() == len(eventos_reducidos)


def test_actividad_cuadra_con_plata(plata, eventos_reducidos):
    act = agregar_actividad(plata).cache()
    assert act.agg(F.sum("eventos")).first()[0] == len(eventos_reducidos)
    por_tipo = {r["tipo"]: r["n"] for r in act.groupBy("tipo").agg(F.sum("eventos").alias("n")).collect()}
    assert por_tipo == dict(Counter(e["type"] for e in eventos_reducidos))
    fila = act.first()
    assert (fila["ventana_fin"] - fila["ventana_inicio"]).total_seconds() == 300


def test_repos_cuenta_estrellas_forks_y_commits(plata, eventos_reducidos):
    repos = agregar_repos(plata)
    tot = repos.agg(F.sum("estrellas"), F.sum("forks"), F.sum("eventos"), F.sum("commits")).first()
    tipos = Counter(e["type"] for e in eventos_reducidos)
    assert tot[0] == tipos["WatchEvent"]
    assert tot[1] == tipos["ForkEvent"]
    assert tot[2] == len(eventos_reducidos)
    assert tot[3] == sum(e["payload"]["size"] for e in eventos_reducidos if e["type"] == "PushEvent")


def test_lenguajes_solo_eventos_de_pr(plata, eventos_reducidos):
    leng = agregar_lenguajes(plata)
    esperado = Counter(e["payload"]["language"] for e in eventos_reducidos if e["payload"].get("language"))
    obtenido = {r["lenguaje"]: r["n"] for r in leng.groupBy("lenguaje").agg(F.sum("eventos_pr").alias("n")).collect()}
    assert obtenido == dict(esperado)


def _ts(texto: str) -> datetime:
    return datetime.fromisoformat(texto)


def test_pico_de_actividad_detectado(spark):
    historia = spark.createDataFrame(
        [(_ts(f"2025-09-16 10:{m:02d}:00"), "WatchEvent", 100 + (m % 3)) for m in range(0, 60, 5)],
        "ventana_inicio timestamp, tipo string, eventos long")
    lote = spark.createDataFrame(
        [(_ts("2025-09-16 11:00:00"), "WatchEvent", 400), (_ts("2025-09-16 11:05:00"), "WatchEvent", 101)],
        "ventana_inicio timestamp, tipo string, eventos long")
    picos = detectar_picos_actividad(lote, historia).collect()
    assert [(p["ventana_inicio"], p["eventos"]) for p in picos] == [(_ts("2025-09-16 11:00:00"), 400)]
    assert picos[0]["z"] > 3


def test_sin_historia_suficiente_no_hay_pico(spark):
    lote = spark.createDataFrame([(_ts("2025-09-16 11:00:00"), "WatchEvent", 400)],
                                 "ventana_inicio timestamp, tipo string, eventos long")
    assert detectar_picos_actividad(lote, None).count() == 0


def test_pico_de_repo_contra_su_linea_base(spark):
    esquema = ("ventana_inicio timestamp, repo_id long, repo_nombre string, estrellas long, forks long, "
               "lenguaje string")
    historia = spark.createDataFrame(
        [(_ts(f"2025-09-16 {h:02d}:00:00"), 1, "a/estable", 20, 0, "Go") for h in range(0, 10)]
        + [(_ts(f"2025-09-16 {h:02d}:00:00"), 2, "b/nuevo", 1, 0, "Rust") for h in range(0, 10, 3)],
        esquema)
    lote = spark.createDataFrame(
        [(_ts("2025-09-16 10:00:00"), 1, "a/estable", 25, 1, "Go"),     # siempre tiene ~20: no es pico
         (_ts("2025-09-16 10:00:00"), 2, "b/nuevo", 40, 3, "Rust"),     # de ~0,4/h a 40: pico
         (_ts("2025-09-16 10:00:00"), 3, "c/chico", 8, 0, None)],       # menos del mínimo de estrellas
        esquema)
    picos = detectar_picos_repos(lote, historia, primera_ventana="2025-09-16 00:00:00").collect()
    assert [p["repo_nombre"] for p in picos] == ["b/nuevo"]
    assert picos[0]["linea_base"] == pytest.approx(0.4)
    assert picos[0]["veces_sobre_base"] == 40.0


def test_pico_de_repo_requiere_horas_de_historia(spark):
    esquema = ("ventana_inicio timestamp, repo_id long, repo_nombre string, estrellas long, forks long, "
               "lenguaje string")
    lote = spark.createDataFrame([(_ts("2025-09-16 01:00:00"), 2, "b/nuevo", 40, 3, "Rust")], esquema)
    assert detectar_picos_repos(lote, None, primera_ventana="2025-09-16 00:00:00").count() == 0
    assert detectar_picos_repos(lote, None, primera_ventana="2025-09-15 20:00:00").count() == 1
