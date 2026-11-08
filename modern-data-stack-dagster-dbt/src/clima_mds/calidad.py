"""Controles de calidad del Parquet crudo. Funciones puras sobre DuckDB: las usan Dagster y pytest."""

from __future__ import annotations

from dataclasses import dataclass, field

# Récords mundiales: 56,7 °C (Death Valley) y −89,2 °C (Vostok); en décimas de grado.
TEMP_MIN_DECIMAS, TEMP_MAX_DECIMAS = -900, 600
FILAS_MINIMAS = 1_000_000
ELEMENTOS_BASICOS = ("TMAX", "TMIN", "PRCP")


@dataclass
class Control:
    nombre: str
    aprobado: bool
    descripcion: str
    metadatos: dict = field(default_factory=dict)


def controles_crudo(con, fuente: str, anio: int, filas_minimas: int = FILAS_MINIMAS) -> list[Control]:
    """Evalúa un año crudo. `fuente` es una expresión FROM de DuckDB (p. ej. read_parquet('s3://...'))."""
    r = con.execute(
        f"""SELECT count(*) AS filas,
                   count(DISTINCT id) AS estaciones,
                   min(fecha) AS fecha_min, max(fecha) AS fecha_max,
                   count(*) FILTER (WHERE id IS NULL OR fecha IS NULL OR elemento IS NULL OR valor IS NULL) AS nulos,
                   count(*) FILTER (WHERE elemento IN ('TMAX', 'TMIN') AND q_flag IS NULL
                                    AND (valor < {TEMP_MIN_DECIMAS} OR valor > {TEMP_MAX_DECIMAS})) AS temp_fuera,
                   list(DISTINCT elemento) FILTER (WHERE elemento IN {ELEMENTOS_BASICOS}) AS basicos
            FROM {fuente}"""
    ).fetchone()
    filas, estaciones, fmin, fmax, nulos, temp_fuera, basicos = r
    basicos = sorted(basicos or [])
    return [
        Control("filas_minimas", filas >= filas_minimas,
                f"Al menos {filas_minimas:,} observaciones en el año", {"filas": filas, "estaciones": estaciones}),
        Control("fechas_dentro_del_anio",
                fmin is not None and fmin >= anio * 10000 + 101 and fmax <= anio * 10000 + 1231,
                "Todas las fechas caen en el año de la partición", {"fecha_min": fmin, "fecha_max": fmax}),
        Control("claves_sin_nulos", nulos == 0, "id, fecha, elemento y valor sin nulos", {"filas_con_nulos": nulos}),
        Control("elementos_basicos_presentes", basicos == sorted(ELEMENTOS_BASICOS),
                "Hay TMAX, TMIN y PRCP", {"presentes": ", ".join(basicos)}),
        Control("temperatura_en_rango_fisico", temp_fuera == 0,
                "Ninguna TMAX/TMIN sin bandera QFLAG fuera de −90…60 °C", {"filas_fuera_de_rango": temp_fuera}),
    ]
