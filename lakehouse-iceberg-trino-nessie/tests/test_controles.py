import re

import pytest

from lakehouse.controles import aprobado, evaluar, patron_no_articulo, sql_metricas
from lakehouse.limpieza import es_no_articulo

REF = [{"vistas_por_idioma": {"en": 200_000_000, "es": 30_000_000}},
       {"vistas_por_idioma": {"en": 210_000_000, "es": 31_000_000}}]


def candidato(**cambios):
    base = {"horas": 24, "filas": 1000, "claves_distintas": 1000, "vistas_no_positivas": 0, "titulos_nulos": 0,
            "no_articulos": 0, "vistas_por_idioma": {"en": 205_000_000, "es": 29_000_000}}
    return {**base, **cambios}


def test_dia_correcto_pasa():
    assert aprobado(evaluar(candidato(), REF))


@pytest.mark.parametrize("cambio, control", [
    ({"horas": 21}, "24 horas presentes"),
    ({"filas": 1100}, "sin duplicados (hora, dominio, título)"),
    ({"vistas_no_positivas": 3}, "vistas > 0 y título no nulo"),
    ({"no_articulos": 50}, "sin páginas que no son artículos"),
    ({"vistas_por_idioma": {"en": 500_000_000, "es": 29_000_000}}, "volumen plausible (en)"),
    ({"vistas_por_idioma": {"en": 205_000_000}}, "idiomas completos"),
])
def test_cada_defecto_hace_fallar_su_control(cambio, control):
    resultado = evaluar(candidato(**cambio), REF)
    fallidos = [c.nombre for c in resultado if not c.paso]
    assert fallidos == [control]
    assert not aprobado(resultado)


def test_sin_referencia_no_bloquea_por_volumen():
    assert aprobado(evaluar(candidato(), []))


@pytest.mark.parametrize("titulo", ["Special:Search", "especial:Buscar", "Wikipedia:Portada", "Main_Page",
                                    "Usuario_discusión:X", "Lionel_Messi", "Re:Zero", "Portada_(arquitectura)",
                                    "main_Page", "Anexo:Finales"])
def test_patron_sql_coincide_con_la_regla_python(titulo):
    patron = patron_no_articulo().replace("[_ ]", "[_ ]")
    assert bool(re.search(patron, titulo)) == es_no_articulo(titulo)


def test_sql_metricas_filtra_el_dia():
    q = sql_metricas("iceberg_dev.wiki.pageviews_limpio", "2026-07-21")
    assert "TIMESTAMP '2026-07-21 00:00:00'" in q["generales"]
    assert "GROUP BY idioma" in q["por_idioma"]
