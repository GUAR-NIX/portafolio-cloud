import json
import os
import time
from pathlib import Path

import boto3
import pytest


@pytest.mark.integracion
def test_archivo_llega_a_dynamodb_sin_duplicar():
    endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    credenciales = {
        "endpoint_url": endpoint,
        "region_name": "us-east-1",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    s3 = boto3.client("s3", **credenciales)
    tabla = boto3.resource("dynamodb", **credenciales).Table("sismos")
    cubo = "sismos-entradas-local"
    # Estado limpio: la prueba no debe pasar por restos de ejecuciones anteriores.
    with tabla.batch_writer() as lote:
        for item in tabla.scan(ProjectionExpression="id_evento").get("Items", []):
            lote.delete_item(Key=item)
    claves_cuarentena = [f"cuarentena/prueba-{n}.geojson.errores.json" for n in (1, 2)]
    for clave in claves_cuarentena:
        s3.delete_object(Bucket=cubo, Key=clave)

    cuerpo = (Path(__file__).parent / "fixtures/sismos.geojson").read_bytes()
    for numero in (1, 2):
        s3.put_object(Bucket=cubo, Key=f"entrada/prueba-{numero}.geojson", Body=cuerpo)

    # El fixture trae un evento inválido: cada entrega deja su detalle en cuarentena, así que
    # ver ambos objetos confirma que las dos invocaciones terminaron.
    limite = time.time() + 60
    errores = {}
    while time.time() < limite and len(errores) < 2:
        for clave in claves_cuarentena:
            try:
                errores[clave] = json.loads(s3.get_object(Bucket=cubo, Key=clave)["Body"].read())
            except s3.exceptions.NoSuchKey:
                pass
        time.sleep(1)
    assert sorted(errores) == claves_cuarentena
    assert all(len(detalle) == 1 for detalle in errores.values())

    items = tabla.scan().get("Items", [])
    # Dos entregas del mismo archivo y solo tres eventos válidos: cero duplicados.
    assert len(items) == 3, json.dumps(items, default=str)
