"""Esquemas Iceberg de las capas crudo y limpio, y carga idempotente por hora con PyIceberg."""

from __future__ import annotations

import gzip
from datetime import datetime
from pathlib import Path

from lakehouse.catalogo import ESQUEMA
from lakehouse.limpieza import columnas_limpias, limpiar

CRUDO = f"{ESQUEMA}.pageviews_crudo"
LIMPIO = f"{ESQUEMA}.pageviews_limpio"
PROP_HORA = "lakehouse.hora"  # propiedad del snapshot que marca qué hora cargó cada commit (trazabilidad)


def _esquemas():
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType, TimestampType

    crudo = Schema(
        NestedField(1, "hora", TimestampType(), required=False, doc="hora UTC del archivo de origen"),
        NestedField(2, "archivo", StringType(), required=False),
        NestedField(3, "linea", LongType(), required=False, doc="número de línea en el archivo filtrado"),
        NestedField(4, "dominio", StringType(), required=False),
        NestedField(5, "titulo", StringType(), required=False, doc="título tal como llegó"),
        NestedField(6, "vistas", StringType(), required=False, doc="texto sin convertir"),
        NestedField(7, "bytes_respuesta", StringType(), required=False),
    )
    spec_crudo = PartitionSpec(PartitionField(1, 1000, DayTransform(), "hora_day"))
    limpio = Schema(
        NestedField(1, "hora", TimestampType(), required=False),
        NestedField(2, "dominio", StringType(), required=False),
        NestedField(3, "idioma", StringType(), required=False),
        NestedField(4, "plataforma", StringType(), required=False),
        NestedField(5, "titulo", StringType(), required=False, doc="normalizado como MediaWiki"),
        NestedField(6, "vistas", LongType(), required=False),
    )
    # Empieza con partición DIARIA; la demostración la cambia a HORARIA sin reescribir lo anterior.
    spec_limpio = PartitionSpec(PartitionField(3, 1000, IdentityTransform(), "idioma"),
                                PartitionField(1, 1001, DayTransform(), "hora_day"))
    return (crudo, spec_crudo), (limpio, spec_limpio)


def crear_tablas(cat) -> None:
    (crudo, spec_c), (limpio, spec_l) = _esquemas()
    cat.create_namespace_if_not_exists(ESQUEMA)
    props = {"write.parquet.compression-codec": "zstd", "write.target-file-size-bytes": str(128 * 1024 * 1024)}
    cat.create_table_if_not_exists(CRUDO, crudo, partition_spec=spec_c, properties=props)
    cat.create_table_if_not_exists(LIMPIO, limpio, partition_spec=spec_l, properties=props)


def horas_cargadas(tabla) -> set[str]:
    """Horas presentes en los datos de esta tabla (en la rama del catálogo).

    Se leen de los datos y no de los snapshots porque el catálogo REST de Nessie solo conserva el snapshot vigente
    en la metadata de la tabla (el historial está en los commits de Nessie). Leer solo la columna `hora` es barato:
    en cada archivo es casi constante y Parquet la guarda con diccionario.
    """
    import pyarrow.compute as pc

    if tabla.current_snapshot() is None:
        return set()
    horas = pc.unique(tabla.scan(selected_fields=("hora",)).to_arrow()["hora"]).to_pylist()
    return {f"{h:%Y-%m-%dT%H:00}" for h in horas}


def leer_lineas(ruta: Path) -> list[str]:
    with gzip.open(ruta, "rb") as f:
        return [linea.decode("utf-8", "replace").rstrip("\n") for linea in f]


def tabla_crudo(lineas: list[str], hora: datetime, archivo: str):
    import pyarrow as pa

    dominios, titulos, vistas, bytes_r = [], [], [], []
    for linea in lineas:
        p = linea.split(" ", 3)
        p += [None] * (4 - len(p))
        dominios.append(p[0])
        titulos.append(p[1])
        vistas.append(p[2])
        bytes_r.append(p[3])
    n = len(lineas)
    return pa.table({
        "hora": pa.array([hora] * n, pa.timestamp("us")),
        "archivo": pa.array([archivo] * n, pa.string()),
        "linea": pa.array(range(1, n + 1), pa.int64()),
        "dominio": pa.array(dominios, pa.string()),
        "titulo": pa.array(titulos, pa.string()),
        "vistas": pa.array(vistas, pa.string()),
        "bytes_respuesta": pa.array(bytes_r, pa.string()),
    })


def tabla_limpia(lineas: list[str], hora: datetime, lote: str | None = None):
    import pyarrow as pa

    r = limpiar(lineas)
    cols = columnas_limpias(r)
    n = len(cols["titulo"])
    datos = {
        "hora": pa.array([hora] * n, pa.timestamp("us")),
        "dominio": pa.array(cols["dominio"], pa.string()),
        "idioma": pa.array(cols["idioma"], pa.string()),
        "plataforma": pa.array(cols["plataforma"], pa.string()),
        "titulo": pa.array(cols["titulo"], pa.string()),
        "vistas": pa.array(cols["vistas"], pa.int64()),
    }
    if lote is not None:
        datos["lote"] = pa.array([lote] * n, pa.string())
    return pa.table(datos), r
