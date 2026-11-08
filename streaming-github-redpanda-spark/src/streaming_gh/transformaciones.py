"""Lógica de las capas como funciones puras de DataFrame: las usa el pipeline en streaming y las pruebas en lote.

bronce  = mensaje de Kafka tal cual (JSON como texto) + metadatos de Kafka
plata   = evento tipado, validado y deduplicado por id (watermark de 2 h sobre created_at)
oro     = agregados por ventana de tiempo de evento (watermark de 10 min) + detección de picos
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from streaming_gh.config import VENTANA_ACTIVIDAD, VENTANA_HORA

_PERSONA = StructType([StructField("id", LongType()), StructField("login", StringType())])

ESQUEMA_EVENTO = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("created_at", StringType()),
    StructField("actor", _PERSONA),
    StructField("repo", StructType([StructField("id", LongType()), StructField("name", StringType())])),
    StructField("org", _PERSONA),
    StructField("public", BooleanType()),
    StructField("payload", StructType([
        StructField("action", StringType()),
        StructField("ref", StringType()),
        StructField("ref_type", StringType()),
        StructField("size", IntegerType()),
        StructField("distinct_size", IntegerType()),
        StructField("number", LongType()),
        StructField("merged", BooleanType()),
        StructField("language", StringType()),
        StructField("forkee", StringType()),
        StructField("tag", StringType()),
    ])),
])


# ------------------------------------------------------------------------------------------------------------ bronce

def bronce_desde_kafka(df: DataFrame) -> DataFrame:
    """Columnas de la fuente Kafka de Spark (key, value, topic, partition, offset, timestamp, headers)."""
    cabeceras = F.map_from_entries(
        F.transform("headers", lambda h: F.struct(h["key"].alias("k"), h["value"].cast("string").alias("v")))
    )
    valor = F.col("value").cast("string")
    return df.select(
        F.col("key").cast("string").alias("clave"),
        valor.alias("valor"),
        F.col("topic").alias("tema"),
        F.col("partition").alias("particion"),
        F.col("offset"),
        F.col("timestamp").alias("ts_kafka"),
        cabeceras.alias("cabeceras"),
        F.current_timestamp().alias("ts_ingesta"),
        F.to_date(F.get_json_object(valor, "$.created_at")).alias("fecha_evento"),
    )


# ------------------------------------------------------------------------------------------------------------- plata

def plata_desde_bronce(df: DataFrame) -> DataFrame:
    """Parsea y tipa. Los mensajes que no son JSON válido o no traen id/fecha/tipo se descartan aquí."""
    e = F.from_json("valor", ESQUEMA_EVENTO).alias("e")
    return (
        df.select(e, "ts_kafka", "cabeceras", "particion", "offset")
        .select(
            F.col("e.id").cast("long").alias("id"),
            F.col("e.type").alias("tipo"),
            F.to_timestamp("e.created_at").alias("creado_en"),
            F.col("e.actor.id").alias("actor_id"),
            F.col("e.actor.login").alias("actor_login"),
            F.col("e.repo.id").alias("repo_id"),
            F.col("e.repo.name").alias("repo_nombre"),
            F.col("e.org.login").alias("org_login"),
            F.col("e.payload.action").alias("accion"),
            F.col("e.payload.ref").alias("ref"),
            F.col("e.payload.ref_type").alias("ref_tipo"),
            F.col("e.payload.size").alias("commits"),
            F.col("e.payload.distinct_size").alias("commits_distintos"),
            F.col("e.payload.number").alias("numero"),
            F.col("e.payload.merged").alias("pr_fusionado"),
            F.col("e.payload.language").alias("lenguaje"),
            F.coalesce(F.col("cabeceras")["reenvio"] == "1", F.lit(False)).alias("es_reenvio"),
            F.col("cabeceras")["retraso_s"].cast("int").alias("retraso_inyectado_s"),
            F.col("ts_kafka"),
            F.col("particion"),
            F.col("offset"),
        )
        .where(F.col("id").isNotNull() & F.col("creado_en").isNotNull() & F.col("tipo").isNotNull())
        .withColumn("fecha", F.to_date("creado_en"))
    )


def deduplicar(df: DataFrame, watermark: str) -> DataFrame:
    """Un id se recuerda mientras su created_at esté dentro del watermark (estado acotado en RocksDB).
    En lote (pruebas, reprocesos) no hay watermark: se usa dropDuplicates, que es lo mismo sin límite de estado."""
    if not df.isStreaming:
        return df.dropDuplicates(["id"])
    return df.withWatermark("creado_en", watermark).dropDuplicatesWithinWatermark(["id"])


# --------------------------------------------------------------------------------------------------------------- oro

def _contar(condicion: Column) -> Column:
    return F.sum(F.when(condicion, 1).otherwise(0)).cast("long")


def _ventana(df: DataFrame, duracion: str, watermark: str | None) -> DataFrame:
    if watermark:
        df = df.withWatermark("creado_en", watermark)
    return df.withColumn("v", F.window("creado_en", duracion))


def _columnas_ventana() -> list[Column]:
    return [F.col("v.start").alias("ventana_inicio"), F.col("v.end").alias("ventana_fin")]


def agregar_actividad(df: DataFrame, watermark: str | None = None) -> DataFrame:
    """Eventos por tipo cada 5 minutos (la actividad por hora del día sale de sumar estas ventanas)."""
    return (
        _ventana(df, VENTANA_ACTIVIDAD, watermark)
        .groupBy("v", "tipo")
        .agg(
            F.count("*").alias("eventos"),
            F.approx_count_distinct("actor_id").alias("actores_aprox"),
            F.approx_count_distinct("repo_id").alias("repos_aprox"),
            F.max("ts_kafka").alias("ts_kafka_max"),
        )
        .select(*_columnas_ventana(), "tipo", "eventos", "actores_aprox", "repos_aprox", "ts_kafka_max")
    )


def agregar_repos(df: DataFrame, watermark: str | None = None) -> DataFrame:
    """Actividad por repositorio y hora: base de los repos en tendencia (estrellas = WatchEvent)."""
    tipo, accion = F.col("tipo"), F.col("accion")
    es_pr = tipo == "PullRequestEvent"
    return (
        _ventana(df, VENTANA_HORA, watermark)
        .groupBy("v", "repo_id", "repo_nombre")
        .agg(
            F.count("*").alias("eventos"),
            _contar(tipo == "WatchEvent").alias("estrellas"),
            _contar(tipo == "ForkEvent").alias("forks"),
            _contar(tipo == "PushEvent").alias("pushes"),
            F.sum(F.when(tipo == "PushEvent", F.col("commits")).otherwise(0)).cast("long").alias("commits"),
            _contar(es_pr & (accion == "opened")).alias("prs_abiertos"),
            _contar(es_pr & (accion == "closed") & F.col("pr_fusionado")).alias("prs_fusionados"),
            _contar((tipo == "IssuesEvent") & (accion == "opened")).alias("issues_abiertos"),
            F.approx_count_distinct("actor_id").alias("actores_aprox"),
            F.max("lenguaje").alias("lenguaje"),
            F.max("ts_kafka").alias("ts_kafka_max"),
        )
        .select(*_columnas_ventana(), "repo_id", "repo_nombre", "eventos", "estrellas", "forks", "pushes", "commits",
                "prs_abiertos", "prs_fusionados", "issues_abiertos", "actores_aprox", "lenguaje", "ts_kafka_max")
    )


def agregar_lenguajes(df: DataFrame, watermark: str | None = None) -> DataFrame:
    """Actividad de pull requests por lenguaje del repositorio (el payload de PR trae base.repo.language)."""
    accion = F.col("accion")
    es_pr = F.col("tipo") == "PullRequestEvent"
    return (
        _ventana(df.where(F.col("lenguaje").isNotNull()), VENTANA_HORA, watermark)
        .groupBy("v", "lenguaje")
        .agg(
            F.count("*").alias("eventos_pr"),
            _contar(es_pr & (accion == "opened")).alias("prs_abiertos"),
            _contar(es_pr & (accion == "closed") & F.col("pr_fusionado")).alias("prs_fusionados"),
            F.approx_count_distinct("repo_id").alias("repos_aprox"),
            F.approx_count_distinct("actor_id").alias("actores_aprox"),
            F.max("ts_kafka").alias("ts_kafka_max"),
        )
        .select(*_columnas_ventana(), "lenguaje", "eventos_pr", "prs_abiertos", "prs_fusionados", "repos_aprox",
                "actores_aprox", "ts_kafka_max")
    )


# ---------------------------------------------------------------------------------------------------------- picos

def detectar_picos_actividad(lote: DataFrame, historia: DataFrame | None, ventanas_base: int = 12,
                             umbral_z: float = 3.0, minimo_eventos: int = 50) -> DataFrame:
    """Ventanas de 5 min cuyo volumen de un tipo supera la media de las `ventanas_base` anteriores en `umbral_z`
    desviaciones. La historia sale de la propia tabla oro (estado en Delta, no en memoria)."""
    columnas = ["ventana_inicio", "tipo", "eventos"]
    base = lote.select(*columnas).withColumn("del_lote", F.lit(True))
    if historia is not None:
        base = base.unionByName(historia.select(*columnas).withColumn("del_lote", F.lit(False)))
    w = Window.partitionBy("tipo").orderBy("ventana_inicio").rowsBetween(-ventanas_base, -1)
    return (
        base.withColumn("media_base", F.avg("eventos").over(w))
        .withColumn("desv_base", F.stddev_samp("eventos").over(w))
        .withColumn("n_base", F.count("eventos").over(w))
        .withColumn("z", (F.col("eventos") - F.col("media_base")) / F.greatest(F.col("desv_base"), F.lit(1.0)))
        .where(F.col("del_lote") & (F.col("n_base") >= ventanas_base // 2) & (F.col("z") >= umbral_z)
               & (F.col("eventos") >= minimo_eventos))
        .select("ventana_inicio", "tipo", "eventos", F.round("media_base", 1).alias("media_base"),
                F.round("desv_base", 1).alias("desv_base"), F.round("z", 2).alias("z"))
    )


def detectar_picos_repos(lote: DataFrame, historia: DataFrame | None, primera_ventana, horas_base: int = 24,
                         minimo_estrellas: int = 15, factor: float = 5.0, horas_minimas: int = 3) -> DataFrame:
    """Repos cuya cantidad de estrellas en una hora es >= `minimo_estrellas` y >= `factor` veces su media por hora
    de las `horas_base` horas anteriores (las horas sin actividad cuentan como 0)."""
    candidatos = lote.where(F.col("estrellas") >= minimo_estrellas).select(
        "ventana_inicio", "repo_id", "repo_nombre", "estrellas", "forks", "lenguaje")
    horas_previas = F.least(
        F.lit(horas_base),
        ((F.col("c.ventana_inicio").cast("long") - F.lit(primera_ventana).cast("timestamp").cast("long")) / 3600)
        .cast("int"),
    )
    if historia is None:
        suma = candidatos.alias("c").withColumn("estrellas_previas", F.lit(0).cast("long"))
    else:
        h = historia.select("ventana_inicio", "repo_id", "estrellas").alias("h")
        suma = (
            candidatos.alias("c")
            .join(h, (F.col("c.repo_id") == F.col("h.repo_id"))
                  & (F.col("h.ventana_inicio") < F.col("c.ventana_inicio"))
                  & (F.col("h.ventana_inicio") >= F.col("c.ventana_inicio") - F.expr(f"INTERVAL {horas_base} HOURS")),
                  "left")
            .groupBy("c.ventana_inicio", "c.repo_id", "c.repo_nombre", "c.estrellas", "c.forks", "c.lenguaje")
            .agg(F.coalesce(F.sum("h.estrellas"), F.lit(0)).cast("long").alias("estrellas_previas"))
            .alias("c")
        )
    return (
        suma.withColumn("horas_previas", horas_previas)
        .where(F.col("horas_previas") >= horas_minimas)
        .withColumn("linea_base", F.col("estrellas_previas") / F.col("horas_previas"))
        .withColumn("veces_sobre_base", F.col("estrellas") / F.greatest(F.col("linea_base"), F.lit(1.0)))
        .where(F.col("veces_sobre_base") >= factor)
        .select("ventana_inicio", "repo_id", "repo_nombre", "estrellas", "forks", "lenguaje",
                F.round("linea_base", 2).alias("linea_base"), F.round("veces_sobre_base", 1).alias("veces_sobre_base"))
    )
