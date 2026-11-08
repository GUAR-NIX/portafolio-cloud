"""Controles de calidad en SQL propio.

Cada control es una consulta que devuelve UN número y un rango aceptable. Los resultados se guardan en
`auditoria.controles_calidad` (para un tablero de calidad) y, si algún control crítico queda fuera de rango,
se lanza `ErrorCalidad`: el DAG falla y los datos malos no llegan a los marts.

Por qué SQL propio y no Great Expectations / Soda: son ~25 reglas sencillas sobre tablas pequeñas en la misma
base; GX agrega cientos de MB de dependencias que chocan con las restricciones de versiones de Airflow, y Soda
Core necesita otro archivo de configuración y otro conector. Aquí cada regla es una línea de SQL legible y
probada con pytest. Si el equipo creciera, la misma lista se traduce 1:1 a checks de Soda (`row_count`,
`invalid_count`, `duplicate_count`).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from ingesta import config

log = logging.getLogger(__name__)


class ErrorCalidad(Exception):
    """Algún control crítico quedó fuera de rango. Lleva todos los resultados para poder registrarlos."""

    def __init__(self, mensaje: str, resultados: list | None = None):
        super().__init__(mensaje)
        self.resultados = resultados or []


@dataclass(frozen=True)
class Control:
    nombre: str
    sql: str                      # debe devolver una sola fila con un solo número
    minimo: float | None = 0      # None = sin límite
    maximo: float | None = 0
    critico: bool = True
    descripcion: str = ""


@dataclass
class Resultado:
    control: Control
    valor: float | None
    ok: bool
    detalle: dict = field(default_factory=dict)


def dentro_de_rango(valor: float | None, minimo: float | None, maximo: float | None) -> bool:
    """Un valor nulo (la consulta no devolvió nada) cuenta como fallo."""
    if valor is None:
        return False
    return (minimo is None or valor >= minimo) and (maximo is None or valor <= maximo)


def evaluar_controles(cursor, controles: list[Control], parametros: dict) -> list[Resultado]:
    resultados = []
    for c in controles:
        cursor.execute(c.sql, parametros)
        fila = cursor.fetchone()
        valor = float(fila[0]) if fila and fila[0] is not None else None
        r = Resultado(c, valor, dentro_de_rango(valor, c.minimo, c.maximo))
        resultados.append(r)
        nivel = logging.INFO if r.ok else (logging.ERROR if c.critico else logging.WARNING)
        log.log(nivel, "%-5s %-40s valor=%s rango=[%s, %s]", "OK" if r.ok else "FALLA", c.nombre, valor,
                c.minimo, c.maximo)
    return resultados


def registrar_resultados(cursor, resultados: list[Resultado], capa: str, particion: str,
                         dag_id: str = "manual", run_id: str = "manual") -> None:
    for r in resultados:
        c = r.control
        cursor.execute(
            """INSERT INTO auditoria.controles_calidad
                   (dag_id, run_id, capa, particion, control, critico, valor, minimo, maximo, ok)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (dag_id, run_id, capa, particion, c.nombre, c.critico, r.valor, c.minimo, c.maximo, r.ok))


def exigir(resultados: list[Resultado], capa: str, particion: str) -> None:
    fallidos = [r for r in resultados if not r.ok and r.control.critico]
    if fallidos:
        resumen = [{"control": r.control.nombre, "valor": r.valor, "minimo": r.control.minimo,
                    "maximo": r.control.maximo} for r in fallidos]
        raise ErrorCalidad(f"{len(fallidos)} control(es) crítico(s) fallaron en {capa}/{particion}: "
                           f"{json.dumps(resumen, ensure_ascii=False)}", resultados)


def ejecutar_controles(cursor, controles: list[Control], parametros: dict, capa: str, particion: str,
                       dag_id: str = "manual", run_id: str = "manual") -> list[Resultado]:
    """Evalúa, registra cada resultado en auditoría y lanza ErrorCalidad si falla alguno crítico."""
    resultados = evaluar_controles(cursor, controles, parametros)
    registrar_resultados(cursor, resultados, capa, particion, dag_id, run_id)
    exigir(resultados, capa, particion)
    return resultados


# ------------------------------------------------------------------------------------------ staging · clima
def controles_clima() -> list[Control]:
    n = len(config.CIUDADES)
    t = "staging.clima_diario WHERE fecha = %(fecha)s"
    return [
        Control("filas_por_dia", f"SELECT count(*) FROM {t}", n, n,
                descripcion="Una fila por ciudad y día"),
        Control("ciudades_distintas", f"SELECT count(DISTINCT ciudad) FROM {t}", n, n),
        Control("nulos_en_temperatura_o_lluvia",
                f"SELECT count(*) FROM {t} AND (temp_max_c IS NULL OR temp_min_c IS NULL OR temp_media_c IS NULL "
                f"OR precipitacion_mm IS NULL)"),
        Control("temp_min_media_max_ordenadas",
                f"SELECT count(*) FROM {t} AND NOT (temp_min_c <= temp_media_c AND temp_media_c <= temp_max_c)"),
        Control("temperatura_en_rango_fisico",
                f"SELECT count(*) FROM {t} AND (temp_min_c < -15 OR temp_max_c > 45)",
                descripcion="Ecuador: ni -15 °C en las ciudades ni más de 45 °C"),
        Control("precipitacion_en_rango", f"SELECT count(*) FROM {t} AND (precipitacion_mm < 0 OR "
                                          f"precipitacion_mm > 400)"),
        Control("humedad_en_rango", f"SELECT count(*) FROM {t} AND (humedad_media_pct < 0 OR "
                                    f"humedad_media_pct > 100)"),
        Control("costa_mas_calida_que_quito",
                "SELECT (SELECT temp_media_c FROM staging.clima_diario WHERE fecha = %(fecha)s AND ciudad = "
                "'Guayaquil') - (SELECT temp_media_c FROM staging.clima_diario WHERE fecha = %(fecha)s AND "
                "ciudad = 'Quito')", 3, None,
                descripcion="Detecta ciudades cruzadas: Guayaquil siempre es varios grados más cálida que Quito"),
        Control("dato_provisional", f"SELECT count(*) FROM {t} AND fuente = 'pronostico'", 0, 0, critico=False,
                descripcion="Advertencia: el día vino de la API de pronóstico y se reemplazará más adelante"),
    ]


# ---------------------------------------------------------------------------------- staging · Banco Mundial
def controles_banco_mundial() -> list[Control]:
    t = "staging.indicadores_bm WHERE anio = %(anio)s"
    controles = [
        Control("hay_filas", f"SELECT count(*) FROM {t}", 1, None),
        Control("ecuador_presente", f"SELECT count(*) FROM {t} AND pais_iso3 = 'ECU'", 1, None),
        Control("paises_esperados", f"SELECT count(*) FROM {t} AND pais_iso3 <> ALL(%(paises)s)",
                descripcion="Ningún país fuera de la lista pedida"),
        Control("sin_duplicados",
                f"SELECT count(*) FROM (SELECT 1 FROM {t} GROUP BY pais_iso3, indicador HAVING count(*) > 1) d"),
        Control("indicadores_esperados", f"SELECT count(*) FROM {t} AND indicador <> ALL(%(indicadores)s)"),
        Control("cobertura_indicadores", f"SELECT count(DISTINCT indicador) FROM {t}",
                len(config.INDICADORES) - 4, None, critico=False,
                descripcion="Advertencia: los años recientes todavía no tienen todos los indicadores"),
    ]
    for codigo, (columna, minimo, maximo) in config.INDICADORES.items():
        controles.append(Control(
            f"rango_{columna}",
            f"SELECT count(*) FROM {t} AND indicador = '{codigo}' AND (valor < {minimo} OR valor > {maximo})"))
    return controles


def parametros_banco_mundial(anio: int) -> dict:
    return {"anio": anio, "paises": config.PAISES, "indicadores": list(config.INDICADORES)}


# ------------------------------------------------------------------------------------------------- marts
def controles_marts() -> list[Control]:
    return [
        Control("fct_clima_concilia_con_staging",
                "SELECT (SELECT count(*) FROM marts.fct_clima_diario) - (SELECT count(*) FROM staging.clima_diario)",
                descripcion="El mart no pierde ni duplica días"),
        Control("fct_clima_sin_duplicados",
                "SELECT count(*) FROM (SELECT 1 FROM marts.fct_clima_diario GROUP BY fecha, ciudad "
                "HAVING count(*) > 1) d"),
        Control("fct_clima_sin_ciudades_huerfanas",
                "SELECT count(*) FROM marts.fct_clima_diario f LEFT JOIN marts.dim_ciudad d USING (ciudad) "
                "WHERE d.ciudad IS NULL"),
        Control("mensual_suma_dias",
                "SELECT (SELECT coalesce(sum(dias_con_dato), 0) FROM marts.clima_mensual_ciudad) - "
                "(SELECT count(*) FROM marts.fct_clima_diario)"),
        Control("mensual_lluvia_cuadra",
                "SELECT abs((SELECT coalesce(sum(lluvia_total_mm), 0) FROM marts.clima_mensual_ciudad) - "
                "(SELECT coalesce(sum(precipitacion_mm), 0) FROM marts.fct_clima_diario))", 0, 0.5),
        Control("indicadores_unicos_por_pais_anio",
                "SELECT count(*) FROM (SELECT 1 FROM marts.indicadores_pais_anual GROUP BY pais_iso3, anio "
                "HAVING count(*) > 1) d"),
        Control("indicadores_concilian",
                "SELECT (SELECT count(*) FROM staging.indicadores_bm) - (SELECT "
                + " + ".join(f"count({c[0]})" for c in config.INDICADORES.values())
                + " FROM marts.indicadores_pais_anual)",
                descripcion="Cada valor no nulo de staging aparece en una celda del mart ancho"),
    ]
