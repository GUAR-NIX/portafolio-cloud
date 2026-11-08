"""Verificación de una corrida con conteos: ¿se perdió o se duplicó algo entre el productor y oro?

Compara, en lote y sobre las tablas Delta finales:
  productor (enviados, ids únicos) ↔ Kafka (offsets) ↔ bronce (filas, offsets únicos) ↔ plata (filas, ids únicos)
  ↔ oro (suma por ventana) ↔ oro recalculado en lote desde plata.

    python -m streaming_gh.verificar --corrida recuperacion     → reports/metricas/<corrida>/verificacion.json
"""

from __future__ import annotations

import argparse
import json
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from streaming_gh.config import KAFKA_BOOTSTRAP, Corrida
from streaming_gh.spark_comun import crear_sesion
from streaming_gh.transformaciones import agregar_actividad, agregar_repos

TARDIO_FUERA_S = 20 * 60  # el productor inyecta tardíos de 20–40 min, más que el watermark de oro (10 min)


def mensajes_en_kafka(tema: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    consumidor = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "verificador"})
    try:
        meta = consumidor.list_topics(tema, timeout=30).topics[tema]
        total = 0
        for p in meta.partitions:
            bajo, alto = consumidor.get_watermark_offsets(TopicPartition(tema, p), timeout=30)
            total += alto - bajo
        return total
    finally:
        consumidor.close()


def verificar(corrida: Corrida, con_kafka: bool = True) -> dict:
    spark = crear_sesion(f"verificar-{corrida.nombre}")
    leer = lambda capa, tabla: spark.read.format("delta").load(corrida.tabla(capa, tabla))  # noqa: E731
    productor_json = corrida.dir_metricas / "productor.json"
    productor = json.loads(productor_json.read_text()) if productor_json.exists() else {}
    r: dict = {"corrida": corrida.nombre, "productor": {k: productor.get(k) for k in (
        "enviados", "originales", "ids_unicos", "duplicados", "tardios_dentro_watermark", "tardios_fuera_watermark")}}

    if con_kafka:
        r["kafka_mensajes"] = mensajes_en_kafka(corrida.tema)

    bronce = leer("bronce", "eventos")
    b = bronce.agg(F.count("*").alias("filas"),
                   F.countDistinct("particion", "offset").alias("offsets_unicos")).first()
    r["bronce"] = {"filas": b["filas"], "offsets_unicos": b["offsets_unicos"],
                   "duplicados": b["filas"] - b["offsets_unicos"]}

    plata = leer("plata", "eventos").cache()
    p = plata.agg(F.count("*").alias("filas"), F.countDistinct("id").alias("ids"),
                  F.sum(F.col("es_reenvio").cast("int")).alias("reenvios"),
                  F.sum((F.col("retraso_inyectado_s") >= TARDIO_FUERA_S).cast("int")).alias("tardios_fuera"),
                  F.sum((F.col("retraso_inyectado_s") < TARDIO_FUERA_S).cast("int")).alias("tardios_dentro"),
                  F.max("creado_en").alias("max_evento")).first()
    r["plata"] = {"filas": p["filas"], "ids_unicos": p["ids"], "duplicados": p["filas"] - p["ids"],
                  "reenvios_que_pasaron": p["reenvios"], "tardios_dentro": p["tardios_dentro"],
                  "tardios_fuera": p["tardios_fuera"]}
    if productor:
        r["plata"]["perdidos_vs_productor"] = productor["ids_unicos"] - p["ids"]
        r["bronce"]["diferencia_vs_enviados"] = b["filas"] - productor["enviados"]

    # Oro: solo las ventanas ya cerradas (el watermark nunca pasa la última hora: no llegan eventos posteriores)
    for tabla, agregar, columna in [("actividad_5min", agregar_actividad, "eventos"),
                                    ("repos_hora", agregar_repos, "eventos")]:
        if not DeltaTable.isDeltaTable(spark, corrida.tabla("oro", tabla)):
            r[f"oro_{tabla}"] = {"ventanas": 0}
            continue
        oro = leer("oro", tabla)
        ultima = oro.agg(F.max("ventana_fin")).first()[0]
        if ultima is None:
            r[f"oro_{tabla}"] = {"ventanas": 0}
            continue
        # repo_nombre en la clave: un repo renombrado dentro de la hora aparece con sus dos nombres (filas distintas)
        claves = (["ventana_inicio", "tipo"] if tabla == "actividad_5min"
                  else ["ventana_inicio", "repo_id", "repo_nombre"])
        lote = agregar(plata.where(F.col("creado_en") < F.lit(ultima))).select(*claves, F.col(columna).alias("lote"))
        flujo = oro.select(*claves, F.col(columna).alias("flujo"))
        dup_ventanas = flujo.count() - flujo.select(*claves).distinct().count()
        comp = lote.join(flujo, claves, "full_outer").fillna(0, ["lote", "flujo"])
        c = comp.agg(F.sum("lote").alias("lote"), F.sum("flujo").alias("flujo"),
                     F.count("*").alias("ventanas"),
                     F.sum((F.col("lote") != F.col("flujo")).cast("int")).alias("distintas"),
                     F.sum((F.col("flujo") > F.col("lote")).cast("int")).alias("flujo_mayor")).first()
        # ¿la diferencia son exactamente los tardíos descartados por el watermark de oro?
        tardios_cerradas = plata.where((F.col("creado_en") < F.lit(ultima))
                                       & (F.col("retraso_inyectado_s") >= TARDIO_FUERA_S)).count()
        r[f"oro_{tabla}"] = {"ultima_ventana_cerrada": str(ultima), "ventanas": c["ventanas"],
                             "ventanas_duplicadas": dup_ventanas, "eventos_en_plata": c["lote"],
                             "eventos_en_oro": c["flujo"], "diferencia": c["lote"] - c["flujo"],
                             "ventanas_con_diferencia": c["distintas"],
                             "ventanas_oro_mayor_que_plata": c["flujo_mayor"],
                             "tardios_fuera_watermark_en_esas_ventanas": tardios_cerradas}
    for tabla in ("lenguajes_hora", "picos_actividad", "picos_repos"):
        try:
            r[f"oro_{tabla}"] = {"filas": leer("oro", tabla).count()}
        except Exception:  # noqa: BLE001 - la tabla de picos solo existe si hubo algún pico
            r[f"oro_{tabla}"] = {"filas": 0}

    exacto = (r["bronce"]["duplicados"] == 0 and r["plata"]["duplicados"] == 0
              and r["plata"].get("perdidos_vs_productor", 0) == 0
              and (not con_kafka or r["kafka_mensajes"] == r["bronce"]["filas"])
              and all(r[f"oro_{t}"].get(k, 0) == 0 for t in ("actividad_5min", "repos_hora")
                      for k in ("ventanas_duplicadas", "ventanas_oro_mayor_que_plata")))
    r["exactamente_una_vez"] = exacto
    spark.stop()
    return r


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corrida", required=True)
    parser.add_argument("--sin-kafka", action="store_true")
    args = parser.parse_args()
    corrida = Corrida(args.corrida)
    r = verificar(corrida, con_kafka=not args.sin_kafka)
    corrida.dir_metricas.mkdir(parents=True, exist_ok=True)
    (corrida.dir_metricas / "verificacion.json").write_text(json.dumps(r, indent=2, ensure_ascii=False))
    print(json.dumps(r, indent=2, ensure_ascii=False))
    return 0 if r["exactamente_una_vez"] else 3


if __name__ == "__main__":
    sys.exit(main())
