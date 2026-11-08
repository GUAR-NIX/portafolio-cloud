"""Lambda S3 -> validación -> DynamoDB -> SNS."""

from __future__ import annotations

import json
import os
import urllib.parse

import boto3

from serverless_sismos.adaptadores_aws import crear_alertador_sns, crear_guardador_dynamodb
from serverless_sismos.procesamiento import procesar_geojson


def _cliente(servicio: str):
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    return boto3.client(servicio, endpoint_url=endpoint)


def manejador(evento, _contexto):
    s3 = _cliente("s3")
    dynamodb = boto3.resource("dynamodb", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    sns = _cliente("sns")
    tabla = dynamodb.Table(os.environ["TABLA_SISMOS"])
    alertar = crear_alertador_sns(sns, os.environ["ARN_TEMA_ALERTAS"])
    resultados = []
    for registro in evento.get("Records", []):
        cubo = registro["s3"]["bucket"]["name"]
        clave = urllib.parse.unquote_plus(registro["s3"]["object"]["key"])
        cuerpo = s3.get_object(Bucket=cubo, Key=clave)["Body"].read()
        documento = json.loads(cuerpo)
        resumen = procesar_geojson(documento, crear_guardador_dynamodb(tabla), alertar)
        if resumen["invalidos"]:
            s3.put_object(
                Bucket=cubo,
                Key=f"cuarentena/{clave.rsplit('/', 1)[-1]}.errores.json",
                Body=json.dumps(resumen["errores"], ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
        resultados.append({"clave": clave, **resumen})
        # Log estructurado: CloudWatch Logs conserva el conteo de cada archivo.
        conteo = {k: v for k, v in resumen.items() if k != "errores"}
        print(json.dumps({"tipo": "resumen_lote", "clave": clave, **conteo}, ensure_ascii=False))
    return {"statusCode": 200, "body": json.dumps(resultados, ensure_ascii=False)}

