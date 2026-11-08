"""Activos de ingesta: descarga por año (particionada), Parquet crudo en MinIO y metadatos de estaciones."""

import gzip
import json
import time

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    Backoff,
    Config,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    StaticPartitionsDefinition,
    asset,
)

from clima_mds import almacen, calidad
from clima_mds import configuracion as cfg
from clima_mds.descarga import descargar_reanudable

particiones_anio = StaticPartitionsDefinition(cfg.ANIOS)
PREFIJO = ["ghcn"]  # coincide con el nombre de la fuente dbt: así dagster-dbt enlaza el linaje
reintentos_red = RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL)

CONTROLES_CRUDO = [
    ("filas_minimas", "Al menos 1 millón de observaciones"),
    ("fechas_dentro_del_anio", "Fechas dentro del año de la partición"),
    ("claves_sin_nulos", "Sin nulos en id, fecha, elemento y valor"),
    ("elementos_basicos_presentes", "Hay TMAX, TMIN y PRCP"),
    ("temperatura_en_rango_fisico", "TMAX/TMIN válidas entre −90 y 60 °C"),
]


def guardar_reporte(nombre: str, datos: dict) -> None:
    destino = cfg.DIR_REPORTES / "ingesta" / f"{nombre}.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(datos, indent=2, ensure_ascii=False), encoding="utf-8")


def bytes_descomprimidos(ruta) -> int:
    total = 0
    with gzip.open(ruta, "rb") as f:
        while bloque := f.read(8 * 1024 * 1024):
            total += len(bloque)
    return total


@asset(
    key_prefix=PREFIJO,
    group_name="ingesta",
    partitions_def=particiones_anio,
    retry_policy=reintentos_red,
    kinds={"python"},
    description="CSV comprimido del año desde NOAA GHCN-Daily en AWS Open Data (descarga reanudable).",
)
def ghcn_csv_anual(context: AssetExecutionContext) -> MaterializeResult:
    anio = context.partition_key
    url = f"{cfg.URL_BASE_NOAA}/csv.gz/by_year/{anio}.csv.gz"
    res = descargar_reanudable(url, cfg.DIR_CRUDO / "by_year" / f"{anio}.csv.gz")
    t0 = time.perf_counter()
    descomprimido = bytes_descomprimidos(res.ruta)
    datos = {
        "anio": anio,
        "url": url,
        "etag": res.etag,
        "bytes_csv_gz": res.bytes_totales,
        "bytes_csv_descomprimido": descomprimido,
        "bytes_descargados_en_esta_corrida": res.bytes_descargados,
        "reutilizado_de_cache": res.reutilizado,
        "segundos_descarga": round(res.segundos, 1),
        "segundos_medir_descomprimido": round(time.perf_counter() - t0, 1),
    }
    guardar_reporte(f"descarga_{anio}", datos)
    return MaterializeResult(
        metadata={
            "url": MetadataValue.url(url),
            "MB_csv_gz": round(res.bytes_totales / 1e6, 1),
            "MB_csv": round(descomprimido / 1e6, 1),
            "segundos_descarga": datos["segundos_descarga"],
            "reutilizado_de_cache": res.reutilizado,
        }
    )


class ConfigCrudo(Config):
    simular_datos_malos: bool = False
    """Corrompe ~2 % de las TMAX (999,9 °C) para ver cómo fallan los controles."""


@asset(
    key_prefix=PREFIJO,
    group_name="ingesta",
    partitions_def=particiones_anio,
    deps=[ghcn_csv_anual],
    kinds={"duckdb", "parquet", "s3"},
    description="Observaciones diarias del año, tal cual vienen (tipadas), en Parquet zstd en MinIO.",
    check_specs=[
        AssetCheckSpec(nombre, asset=AssetKey(PREFIJO + ["ghcn_crudo_parquet"]), blocking=True, description=desc)
        for nombre, desc in CONTROLES_CRUDO
    ],
)
def ghcn_crudo_parquet(context: AssetExecutionContext, config: ConfigCrudo):
    anio = context.partition_key
    origen = (cfg.DIR_CRUDO / "by_year" / f"{anio}.csv.gz").as_posix()
    destino = cfg.ruta_s3_crudo(anio)
    con = almacen.conectar()
    t0 = time.perf_counter()
    almacen.csv_a_parquet(con, origen, destino, simular_datos_malos=config.simular_datos_malos)
    segundos_conversion = time.perf_counter() - t0
    t1 = time.perf_counter()
    controles = calidad.controles_crudo(con, f"read_parquet('{destino}')", int(anio), calidad.FILAS_MINIMAS)
    segundos_controles = time.perf_counter() - t1
    bytes_parquet = almacen.tamano_objeto_s3(con, destino)
    filas = controles[0].metadatos["filas"]
    estaciones = controles[0].metadatos["estaciones"]
    guardar_reporte(
        f"crudo_{anio}" + ("_datos_malos" if config.simular_datos_malos else ""),
        {
            "anio": anio,
            "destino": destino,
            "filas": filas,
            "estaciones": estaciones,
            "bytes_parquet": bytes_parquet,
            "segundos_conversion": round(segundos_conversion, 1),
            "segundos_controles": round(segundos_controles, 1),
            "simular_datos_malos": config.simular_datos_malos,
            "controles": {c.nombre: {"aprobado": c.aprobado, **c.metadatos} for c in controles},
        },
    )
    yield MaterializeResult(
        metadata={
            "ubicacion": destino,
            "dagster/row_count": filas,
            "estaciones": estaciones,
            "MB_parquet": round(bytes_parquet / 1e6, 1),
            "segundos_conversion": round(segundos_conversion, 1),
        }
    )
    for c in controles:
        yield AssetCheckResult(
            check_name=c.nombre,
            passed=c.aprobado,
            severity=AssetCheckSeverity.ERROR,
            description=c.descripcion,
            metadata={k: (v if isinstance(v, (int, float, str)) else str(v)) for k, v in c.metadatos.items()},
        )


def descargar_texto(nombre: str) -> str:
    destino = cfg.DIR_CRUDO / nombre
    descargar_reanudable(f"{cfg.URL_BASE_NOAA}/{nombre}", destino)
    return destino.read_text(encoding="latin-1")


@asset(key_prefix=PREFIJO, group_name="ingesta", retry_policy=reintentos_red, kinds={"python", "parquet", "s3"},
       description="Catálogo de estaciones (ghcnd-stations.txt, ancho fijo) en Parquet.")
def ghcn_estaciones() -> MaterializeResult:
    df = almacen.parsear_estaciones(descargar_texto("ghcnd-stations.txt"))
    filas = almacen.df_a_parquet(almacen.conectar(), df, cfg.ruta_s3_metadatos("estaciones"))
    return MaterializeResult(metadata={"dagster/row_count": filas})


@asset(key_prefix=PREFIJO, group_name="ingesta", retry_policy=reintentos_red, kinds={"python", "parquet", "s3"},
       description="Códigos FIPS de país (ghcnd-countries.txt) en Parquet.")
def ghcn_paises() -> MaterializeResult:
    df = almacen.parsear_paises(descargar_texto("ghcnd-countries.txt"))
    filas = almacen.df_a_parquet(almacen.conectar(), df, cfg.ruta_s3_metadatos("paises"))
    return MaterializeResult(metadata={"dagster/row_count": filas})
