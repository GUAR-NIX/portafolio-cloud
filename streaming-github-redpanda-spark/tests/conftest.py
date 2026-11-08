"""Fixtures comunes: eventos reales de la muestra (crudos y reducidos) y una sesión de Spark local con Delta."""

from __future__ import annotations

import gzip
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
MUESTRA = RAIZ / "data" / "muestra" / "gharchive_muestra.json.gz"


@pytest.fixture(scope="session")
def eventos_crudos() -> list[dict]:
    with gzip.open(MUESTRA, "rt", encoding="utf-8") as f:
        return [json.loads(linea) for linea in f]


@pytest.fixture(scope="session")
def eventos_reducidos(eventos_crudos) -> list[dict]:
    from streaming_gh.reducir import reducir_evento

    return [reducir_evento(e) for e in eventos_crudos]


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pyspark = pytest.importorskip("pyspark")  # noqa: F841 - fuera del contenedor (Python 3.14) se saltan
    if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
        pytest.skip("Spark necesita Java; con todo incluido: docker compose run --rm pruebas")
    from streaming_gh.spark_comun import crear_sesion

    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    sesion = crear_sesion("pruebas", s3=False, master=os.environ.get("SPARK_MASTER", "local[2]"))
    try:  # Delta necesita sus jars y, en Windows, winutils: el contenedor trae todo, un Java suelto no basta
        sesion.range(1).write.format("delta").save(str(tmp_path_factory.mktemp("sonda_delta") / "tabla"))
    except Exception as error:
        sesion.stop()
        pytest.skip(f"Spark no puede escribir Delta aquí ({type(error).__name__}); "
                    "con todo incluido: docker compose run --rm pruebas")
    yield sesion
    sesion.stop()


def filas_kafka(eventos: list[dict], cabeceras: dict[int, list[tuple[str, bytes]]] | None = None,
                desde_offset: int = 0) -> list[tuple]:
    """Simula lo que entrega la fuente Kafka de Spark: (key, value, topic, partition, offset, timestamp, headers)."""
    cabeceras = cabeceras or {}
    ahora = datetime.now(UTC).replace(tzinfo=None)
    filas = []
    for i, e in enumerate(eventos):
        valor = e if isinstance(e, bytes) else json.dumps(e).encode()
        filas.append((b"1", valor, "gh-eventos-prueba", i % 3, desde_offset + i, ahora,
                      [(k, v) for k, v in cabeceras.get(i, [])] or None))
    return filas


ESQUEMA_KAFKA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp, "
    "headers array<struct<key:string,value:binary>>"
)
