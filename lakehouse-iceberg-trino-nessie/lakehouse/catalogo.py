"""Conexiones: catálogo Iceberg (PyIceberg → Nessie REST) y Trino. Credenciales locales tomadas del entorno."""

from __future__ import annotations

import os

ESQUEMA = "wiki"


def catalogo(rama: str = "main"):
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        f"nessie_{rama}",
        type="rest",
        uri=f"{os.environ.get('NESSIE_URI', 'http://localhost:19120')}/iceberg/{rama}",
        **{
            "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
            "s3.path-style-access": "true",
            "s3.remote-signing-enabled": "false",
        },
    )


def trino(catalogo_trino: str = "iceberg"):
    import trino as cliente

    return cliente.dbapi.connect(host=os.environ.get("TRINO_HOST", "localhost"), port=8080, user="lakehouse",
                                 catalog=catalogo_trino, schema=ESQUEMA)
