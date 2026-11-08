"""Adaptadores pequeños para probar la lógica sin AWS."""

from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError


def crear_guardador_dynamodb(tabla: Any):
    def guardar(item: dict[str, object]) -> bool:
        try:
            tabla.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(id_evento)",
            )
            return True
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise

    return guardar


def crear_alertador_sns(cliente_sns: Any, arn_tema: str):
    def alertar(mensaje: dict[str, object]) -> None:
        cliente_sns.publish(
            TopicArn=arn_tema,
            Subject=f"Sismo M{mensaje['magnitud']} cerca de Ecuador",
            Message=json.dumps(mensaje, ensure_ascii=False),
        )

    return alertar

