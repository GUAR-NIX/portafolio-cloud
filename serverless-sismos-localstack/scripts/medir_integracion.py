"""Mide la integración real S3 -> Lambda -> DynamoDB -> SNS -> SQS contra LocalStack.

Requiere `docker compose up -d --wait` y `terraform apply` previos. Solo acepta endpoints locales.
"""

from __future__ import annotations

import argparse
import copy
import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

import boto3

RAIZ = Path(__file__).resolve().parents[1]
CUBO = "sismos-entradas-local"
TABLA = "sismos"
COLA = "buzon-alertas-sismos"
FUNCION = "ingestar-sismos"
# Prefijo único por ejecución: CloudWatch conserva resúmenes de corridas anteriores.
CORRIDA = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def clientes(endpoint: str) -> dict:
    if urlparse(endpoint).hostname not in {"localhost", "127.0.0.1"}:
        raise SystemExit("Por seguridad solo se mide contra un emulador en localhost.")
    comunes = {
        "endpoint_url": endpoint,
        "region_name": "us-east-1",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    return {
        "s3": boto3.client("s3", **comunes),
        "tabla": boto3.resource("dynamodb", **comunes).Table(TABLA),
        "sqs": boto3.client("sqs", **comunes),
        "logs": boto3.client("logs", **comunes),
        "lambda": boto3.client("lambda", **comunes),
    }


def percentil(valores: list[float], q: float) -> float:
    ordenados = sorted(valores)
    posicion = (len(ordenados) - 1) * q
    bajo = int(posicion)
    alto = min(bajo + 1, len(ordenados) - 1)
    return ordenados[bajo] + (ordenados[alto] - ordenados[bajo]) * (posicion - bajo)


def resumen_latencias(valores: list[float]) -> dict:
    return {
        "n": len(valores),
        "p50_ms": round(percentil(valores, 0.50), 1),
        "p95_ms": round(percentil(valores, 0.95), 1),
        "media_ms": round(statistics.mean(valores), 1),
        "min_ms": round(min(valores), 1),
        "max_ms": round(max(valores), 1),
    }


def contar_items(tabla) -> int:
    total, inicio = 0, None
    while True:
        argumentos = {"Select": "COUNT"}
        if inicio:
            argumentos["ExclusiveStartKey"] = inicio
        pagina = tabla.scan(**argumentos)
        total += pagina["Count"]
        inicio = pagina.get("LastEvaluatedKey")
        if not inicio:
            return total


def limpiar(c: dict, url_cola: str) -> None:
    claves = []
    inicio = None
    while True:
        argumentos = {"ProjectionExpression": "id_evento"}
        if inicio:
            argumentos["ExclusiveStartKey"] = inicio
        pagina = c["tabla"].scan(**argumentos)
        claves += pagina.get("Items", [])
        inicio = pagina.get("LastEvaluatedKey")
        if not inicio:
            break
    with c["tabla"].batch_writer() as lote:
        for clave in claves:
            lote.delete_item(Key=clave)
    for pagina in c["s3"].get_paginator("list_objects_v2").paginate(Bucket=CUBO):
        for objeto in pagina.get("Contents", []):
            c["s3"].delete_object(Bucket=CUBO, Key=objeto["Key"])
    drenar_alertas(c, url_cola, espera_s=1)


def drenar_alertas(c: dict, url_cola: str, espera_s: int = 2) -> list[dict]:
    mensajes = []
    while True:
        respuesta = c["sqs"].receive_message(
            QueueUrl=url_cola, MaxNumberOfMessages=10, WaitTimeSeconds=espera_s
        )
        lote = respuesta.get("Messages", [])
        if not lote:
            return mensajes
        for mensaje in lote:
            mensajes.append(json.loads(mensaje["Body"]))
            c["sqs"].delete_message(QueueUrl=url_cola, ReceiptHandle=mensaje["ReceiptHandle"])


def resumenes_lambda(c: dict) -> dict[str, dict]:
    grupo = f"/aws/lambda/{FUNCION}"
    encontrados: dict[str, dict] = {}
    for pagina in c["logs"].get_paginator("filter_log_events").paginate(logGroupName=grupo):
        for evento in pagina.get("events", []):
            texto = evento["message"].strip()
            if '"resumen_lote"' in texto:
                datos = json.loads(texto[texto.index("{") :])
                encontrados.setdefault(datos["clave"], datos)
    return encontrados


def esperar_resumen(c: dict, clave: str, limite_s: float = 300) -> dict:
    limite = time.time() + limite_s
    while time.time() < limite:
        datos = resumenes_lambda(c).get(clave)
        if datos:
            return datos
        time.sleep(1)
    raise TimeoutError(f"Lambda no registró el resumen de {clave}")


def subir(c: dict, clave: str, documento: dict) -> float:
    cuerpo = json.dumps(documento, ensure_ascii=False).encode("utf-8")
    inicio = time.perf_counter()
    c["s3"].put_object(Bucket=CUBO, Key=clave, Body=cuerpo, ContentType="application/geo+json")
    return inicio


def escenario_volumen(c: dict, url_cola: str, semana: dict) -> dict:
    esperados = len(semana["features"])
    inicio = subir(c, f"entrada/{CORRIDA}-semana-a.geojson", semana)
    while contar_items(c["tabla"]) < esperados:
        time.sleep(0.5)
    duracion = time.perf_counter() - inicio
    primera = esperar_resumen(c, f"entrada/{CORRIDA}-semana-a.geojson")
    alertas_primera = drenar_alertas(c, url_cola)

    subir(c, f"entrada/{CORRIDA}-semana-b.geojson", semana)
    segunda = esperar_resumen(c, f"entrada/{CORRIDA}-semana-b.geojson")
    alertas_segunda = drenar_alertas(c, url_cola)
    return {
        "archivo": "data/raw/usgs_semana.geojson",
        "primera_entrega": primera,
        "segunda_entrega_identica": segunda,
        "items_en_dynamodb": contar_items(c["tabla"]),
        "segundos_hasta_dynamodb_completo": round(duracion, 2),
        "eventos_por_segundo": round(esperados / duracion, 1),
        "alertas_recibidas_en_sqs": len(alertas_primera),
        "alertas_recibidas_en_repeticion": len(alertas_segunda),
        "alertas": alertas_primera,
    }


def corromper(documento: dict) -> tuple[dict, list[str]]:
    """Inyecta cinco errores controlados y documentados sobre eventos reales."""
    copia = copy.deepcopy(documento)
    f = copia["features"]
    cambios = [
        (10, "magnitud nula", lambda e: e["properties"].update(mag=None)),
        (30, "latitud 120", lambda e: e["geometry"]["coordinates"].__setitem__(1, 120.0)),
        (50, "geometría Polygon", lambda e: e["geometry"].update(type="Polygon")),
        (70, "sin identificador", lambda e: e.pop("id")),
        (90, "profundidad 900 km", lambda e: e["geometry"]["coordinates"].__setitem__(2, 900.0)),
    ]
    descripcion = []
    for posicion, texto, aplicar in cambios:
        aplicar(f[posicion])
        descripcion.append(f"posición {posicion}: {texto}")
    return copia, descripcion


def escenario_cuarentena(c: dict, url_cola: str, ecuador: dict) -> dict:
    documento, inyectados = corromper(ecuador)
    clave = f"entrada/{CORRIDA}-ecuador-2024-con-errores.geojson"
    subir(c, clave, documento)
    resumen = esperar_resumen(c, clave)
    alertas = drenar_alertas(c, url_cola)
    objeto = f"cuarentena/{clave.rsplit('/', 1)[-1]}.errores.json"
    errores = json.loads(c["s3"].get_object(Bucket=CUBO, Key=objeto)["Body"].read())
    return {
        "archivo": "data/raw/usgs_ecuador_2024.geojson + 5 errores inyectados",
        "errores_inyectados": inyectados,
        "resumen_lambda": resumen,
        "objeto_cuarentena": objeto,
        "registros_en_cuarentena": len(errores),
        "alertas_recibidas_en_sqs": len(alertas),
        "alertas": alertas,
    }


def forzar_arranque_frio(c: dict, ciclo: int) -> None:
    """Cambiar la configuración obliga a LocalStack a crear un entorno nuevo."""
    variables = c["lambda"].get_function_configuration(FunctionName=FUNCION)["Environment"][
        "Variables"
    ]
    variables["MEDICION_CICLO"] = str(ciclo)
    c["lambda"].update_function_configuration(
        FunctionName=FUNCION, Environment={"Variables": variables}
    )
    c["lambda"].get_waiter("function_updated_v2").wait(FunctionName=FUNCION)
    # Sin esta pausa LocalStack puede cancelar la invocación que llega mientras apaga el
    # entorno anterior (CancelledError) y el evento se pierde.
    time.sleep(5)


def una_medicion(c: dict, url_cola: str, evento: dict, clave: str) -> dict:
    c["tabla"].delete_item(Key={"id_evento": evento["id"]})
    inicio = subir(c, clave, {"type": "FeatureCollection", "features": [evento]})
    t_dynamo = t_alerta = None
    limite = inicio + 120
    while (t_dynamo is None or t_alerta is None) and time.perf_counter() < limite:
        if t_dynamo is None and "Item" in c["tabla"].get_item(Key={"id_evento": evento["id"]}):
            t_dynamo = time.perf_counter()
        if t_alerta is None:
            mensajes = c["sqs"].receive_message(QueueUrl=url_cola, WaitTimeSeconds=0).get(
                "Messages", []
            )
            for mensaje in mensajes:
                c["sqs"].delete_message(QueueUrl=url_cola, ReceiptHandle=mensaje["ReceiptHandle"])
                if json.loads(mensaje["Body"])["id_evento"] == evento["id"]:
                    t_alerta = time.perf_counter()
        time.sleep(0.01)
    if t_dynamo is None or t_alerta is None:
        raise TimeoutError(f"Sin respuesta para {clave}")
    return {
        "clave": clave,
        "id_evento": evento["id"],
        "s3_a_dynamodb_ms": round((t_dynamo - inicio) * 1000, 1),
        "s3_a_alerta_ms": round((t_alerta - inicio) * 1000, 1),
    }


def escenario_latencias(
    c: dict, url_cola: str, eventos_alerta: list[dict], rondas: int, calientes: int
) -> dict:
    mediciones = []
    for ronda in range(rondas):
        forzar_arranque_frio(c, ronda)
        for paso in range(calientes + 1):
            evento = eventos_alerta[(ronda * (calientes + 1) + paso) % len(eventos_alerta)]
            fila = una_medicion(c, url_cola, evento, f"entrada/{CORRIDA}-latencia-{ronda:02d}-{paso:02d}.geojson")
            fila["arranque"] = "frio" if paso == 0 else "caliente"
            mediciones.append(fila)
            print(fila)
    resultado = {"mediciones": mediciones}
    for tipo in ("frio", "caliente"):
        filas = [m for m in mediciones if m["arranque"] == tipo]
        resultado[tipo] = {
            "s3_a_dynamodb": resumen_latencias([m["s3_a_dynamodb_ms"] for m in filas]),
            "s3_a_alerta": resumen_latencias([m["s3_a_alerta_ms"] for m in filas]),
        }
    return resultado


def version_localstack(endpoint: str) -> str:
    with urlopen(f"{endpoint}/_localstack/health", timeout=5) as respuesta:
        return json.loads(respuesta.read()).get("version", "desconocida")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://localhost:4566")
    parser.add_argument("--rondas", type=int, default=5)
    parser.add_argument("--calientes", type=int, default=6)
    parser.add_argument("--salida", type=Path, default=RAIZ / "reports/integracion_localstack.json")
    args = parser.parse_args()

    c = clientes(args.endpoint)
    url_cola = c["sqs"].get_queue_url(QueueName=COLA)["QueueUrl"]
    semana = json.loads((RAIZ / "data/raw/usgs_semana.geojson").read_text(encoding="utf-8"))
    ecuador = json.loads((RAIZ / "data/raw/usgs_ecuador_2024.geojson").read_text(encoding="utf-8"))

    limpiar(c, url_cola)
    volumen = escenario_volumen(c, url_cola, semana)
    print("volumen", {k: v for k, v in volumen.items() if k != "alertas"})
    cuarentena = escenario_cuarentena(c, url_cola, ecuador)
    print("cuarentena", cuarentena["resumen_lambda"])

    ids_alerta = {a["id_evento"] for a in volumen["alertas"] + cuarentena["alertas"]}
    eventos_alerta = [
        e for e in semana["features"] + ecuador["features"] if e.get("id") in ids_alerta
    ]
    latencias = escenario_latencias(c, url_cola, eventos_alerta, args.rondas, args.calientes)

    resultado = {
        "fecha_ejecucion": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "entorno": {
            "emulador": f"LocalStack {version_localstack(args.endpoint)} (community, Docker Desktop en Windows 11)",
            "endpoint": args.endpoint,
            "lambda": "python3.12, 256 MB, contenedor creado por LocalStack",
            "metodo_latencia": (
                "Reloj del cliente: desde put_object en S3 hasta ver el item con get_item y hasta "
                "recibir la alerta en SQS (sondeo cada ~10 ms). Antes de cada medición se borra el "
                "item para que el evento real vuelva a ser nuevo y dispare alerta. Cada vuelta "
                "del sondeo (get_item + receive_message) tarda decenas de ms en el emulador, así "
                "que la resolución es de ~±100 ms y S3→DynamoDB puede verse mayor que S3→alerta."
            ),
            "metodo_arranque_frio": (
                "update_function_configuration antes de la primera subida de cada ronda: LocalStack "
                "descarta el entorno caliente y crea un contenedor nuevo."
            ),
        },
        "volumen_y_duplicados": volumen,
        "cuarentena": cuarentena,
        "latencias": latencias,
    }
    args.salida.write_text(json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: resultado["latencias"][k] for k in ("frio", "caliente")}, indent=2))


if __name__ == "__main__":
    main()
