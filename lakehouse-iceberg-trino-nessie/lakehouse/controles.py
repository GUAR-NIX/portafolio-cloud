"""Controles de calidad de un día cargado en una rama, antes de fusionarlo a `main` (write-audit-publish).

La lógica de decisión es Python puro: recibe métricas ya calculadas (por Trino) del día candidato y de los días
de referencia que ya están en `main`, y devuelve la lista de controles con su resultado. Así se prueba con
pytest sin contenedores; `sql_metricas` arma las consultas que producen esas métricas.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import median

from lakehouse.limpieza import ESPACIOS_NO_ARTICULO, PORTADAS

TOLERANCIA_VOLUMEN = (0.6, 1.5)  # vistas del día frente a la mediana de los días de referencia, por idioma


@dataclass
class Control:
    nombre: str
    paso: bool
    detalle: str


def evaluar(candidato: dict, referencia: list[dict]) -> list[Control]:
    """`candidato` y cada elemento de `referencia` tienen las claves que produce `sql_metricas`."""
    c = []
    horas = candidato["horas"]
    c.append(Control("24 horas presentes", horas == 24, f"{horas} horas distintas"))
    dup = candidato["filas"] - candidato["claves_distintas"]
    c.append(Control("sin duplicados (hora, dominio, título)", dup == 0, f"{dup:,} filas repetidas"))
    c.append(Control("vistas > 0 y título no nulo", candidato["vistas_no_positivas"] == 0
                     and candidato["titulos_nulos"] == 0,
                     f"{candidato['vistas_no_positivas']} vistas ≤ 0, {candidato['titulos_nulos']} títulos nulos"))
    c.append(Control("sin páginas que no son artículos", candidato["no_articulos"] == 0,
                     f"{candidato['no_articulos']} filas de espacios de nombres o portadas"))
    for idioma, vistas in sorted(candidato["vistas_por_idioma"].items()):
        ref = [r["vistas_por_idioma"].get(idioma, 0) for r in referencia]
        if not ref:
            c.append(Control(f"volumen plausible ({idioma})", True, "sin días de referencia"))
            continue
        base = median(ref)
        razon = vistas / base if base else float("inf")
        lo, hi = TOLERANCIA_VOLUMEN
        c.append(Control(f"volumen plausible ({idioma})", lo <= razon <= hi,
                         f"{vistas:,} vistas = {razon:.2f}× la mediana de referencia ({base:,.0f}); "
                         f"rango aceptado {lo}–{hi}×"))
    idiomas_ref = set().union(*(r["vistas_por_idioma"] for r in referencia)) if referencia else set()
    faltan = idiomas_ref - set(candidato["vistas_por_idioma"])
    c.append(Control("idiomas completos", not faltan, f"faltan: {sorted(faltan)}" if faltan else "en y es"))
    return c


def aprobado(controles: list[Control]) -> bool:
    return all(x.paso for x in controles)


def como_dict(controles: list[Control]) -> list[dict]:
    return [asdict(x) for x in controles]


def patron_no_articulo() -> str:
    """Expresión regular (sintaxis de Trino/Java) equivalente a `limpieza.es_no_articulo`."""
    espacios = "|".join(sorted(e.replace("_", "[_ ]") for e in ESPACIOS_NO_ARTICULO))
    portadas = "|".join(p.replace(":", "\\:") for p in sorted(PORTADAS))
    return f"(?i)^(({espacios}):.+|(?-i:{portadas}))$"


def sql_metricas(tabla: str, dia: str) -> dict[str, str]:
    """Consultas de Trino que calculan las métricas de un día en `tabla` (catálogo.esquema.tabla)."""
    filtro = f"hora >= TIMESTAMP '{dia} 00:00:00' AND hora < TIMESTAMP '{dia} 00:00:00' + INTERVAL '1' DAY"
    patron = patron_no_articulo().replace("'", "''")
    return {
        "generales": f"""
            SELECT count(DISTINCT hora) AS horas, count(*) AS filas,
                   count(DISTINCT (hora, dominio, titulo)) AS claves_distintas,
                   count_if(vistas IS NULL OR vistas <= 0) AS vistas_no_positivas,
                   count_if(titulo IS NULL) AS titulos_nulos,
                   count_if(regexp_like(titulo, '{patron}')) AS no_articulos
            FROM {tabla} WHERE {filtro}""",
        "por_idioma": f"SELECT idioma, sum(vistas) FROM {tabla} WHERE {filtro} GROUP BY idioma",
    }
