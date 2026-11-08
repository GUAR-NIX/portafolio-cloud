"""Integración con Postgres real: raw -> staging -> controles -> marts, idempotencia y fallos de calidad.
Corre en el contenedor `pruebas` y en la CI (servicio postgres); sin DATOS_PRUEBAS_URI se omite."""
from datetime import date

import pytest

from ingesta import calidad, config

DIA = date(2025, 10, 15)


@pytest.fixture
def bd():
    from ingesta import bd as modulo
    return modulo


@pytest.fixture
def clima_cargado(conn, bd, respuesta_clima):
    bd.guardar_raw_clima(conn, DIA, {"fuente": "archivo", "url": config.URL_ARCHIVO, "parametros": {},
                                     "respuesta": respuesta_clima})
    bd.cargar_staging_clima(conn, DIA)
    return conn


def contar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def test_staging_clima_y_controles_en_verde(clima_cargado):
    conn = clima_cargado
    assert contar(conn, "SELECT count(*) FROM staging.clima_diario") == 13
    with conn.cursor() as cur:
        resultados = calidad.ejecutar_controles(cur, calidad.controles_clima(), {"fecha": DIA}, "staging", "x")
    assert all(r.ok for r in resultados)
    assert contar(conn, "SELECT count(*) FROM auditoria.controles_calidad WHERE ok") == len(resultados)


def test_recargar_la_misma_fecha_es_idempotente(clima_cargado, bd, respuesta_clima):
    conn = clima_cargado
    antes = contar(conn, "SELECT sum(temp_media_c) FROM staging.clima_diario")
    bd.guardar_raw_clima(conn, DIA, {"fuente": "archivo", "url": "u", "parametros": {}, "respuesta": respuesta_clima})
    bd.cargar_staging_clima(conn, DIA)
    bd.cargar_staging_clima(conn, DIA)
    assert contar(conn, "SELECT count(*) FROM staging.clima_diario") == 13
    assert contar(conn, "SELECT sum(temp_media_c) FROM staging.clima_diario") == antes
    assert contar(conn, "SELECT count(*) FROM raw.clima_respuestas") == 1
    assert contar(conn, "SELECT descargas FROM raw.clima_respuestas") == 2
    assert bd.raw_clima_definitivo(conn, DIA) is True


@pytest.mark.parametrize("corrupcion, control", [
    ("UPDATE staging.clima_diario SET temp_max_c = 80 WHERE ciudad = 'Loja'", "temperatura_en_rango_fisico"),
    ("UPDATE staging.clima_diario SET temp_min_c = temp_max_c + 1 WHERE ciudad = 'Puyo'",
     "temp_min_media_max_ordenadas"),
    ("DELETE FROM staging.clima_diario WHERE ciudad = 'Manta'", "filas_por_dia"),
    ("UPDATE staging.clima_diario SET precipitacion_mm = NULL WHERE ciudad = 'Cuenca'",
     "nulos_en_temperatura_o_lluvia"),
    ("UPDATE staging.clima_diario SET temp_media_c = 30 WHERE ciudad = 'Quito'", "costa_mas_calida_que_quito"),
])
def test_datos_malos_hacen_fallar_los_controles(clima_cargado, corrupcion, control):
    conn = clima_cargado
    with conn.cursor() as cur:
        cur.execute(corrupcion)
        with pytest.raises(calidad.ErrorCalidad, match=control):
            calidad.ejecutar_controles(cur, calidad.controles_clima(), {"fecha": DIA}, "staging", "x")
    conn.commit()
    assert contar(conn, "SELECT count(*) FROM auditoria.controles_calidad WHERE NOT ok AND control = %s",
                  (control,)) == 1


def test_dato_provisional_solo_advierte(conn, bd, respuesta_clima):
    bd.guardar_raw_clima(conn, DIA, {"fuente": "pronostico", "url": "u", "parametros": {},
                                     "respuesta": respuesta_clima})
    bd.cargar_staging_clima(conn, DIA)
    with conn.cursor() as cur:
        resultados = calidad.ejecutar_controles(cur, calidad.controles_clima(), {"fecha": DIA}, "staging", "x")
    fallidos = [r.control.nombre for r in resultados if not r.ok]
    assert fallidos == ["dato_provisional"]
    assert bd.raw_clima_definitivo(conn, DIA) is False


def test_banco_mundial_staging_y_controles(conn, bd, respuesta_bm):
    bd.guardar_raw_banco_mundial(conn, 2023, "NY.GDP.PCAP.CD", respuesta_bm)
    n = bd.cargar_staging_banco_mundial(conn, 2023)
    assert n == sum(r["value"] is not None for r in respuesta_bm)
    with conn.cursor() as cur:
        resultados = calidad.ejecutar_controles(cur, calidad.controles_banco_mundial(),
                                                calidad.parametros_banco_mundial(2023), "staging", "2023")
    assert [r.control.nombre for r in resultados if not r.ok] == ["cobertura_indicadores"]   # solo 1 de 10
    with conn.cursor() as cur:
        cur.execute("UPDATE staging.indicadores_bm SET valor = -5 WHERE pais_iso3 = 'ECU'")
        with pytest.raises(calidad.ErrorCalidad, match="rango_pib_per_capita_usd"):
            calidad.ejecutar_controles(cur, calidad.controles_banco_mundial(),
                                       calidad.parametros_banco_mundial(2023), "staging", "2023")


def test_marts_concilian_con_staging(clima_cargado, bd, respuesta_bm):
    conn = clima_cargado
    bd.guardar_raw_banco_mundial(conn, 2023, "NY.GDP.PCAP.CD", respuesta_bm)
    bd.cargar_staging_banco_mundial(conn, 2023)
    tablas = bd.construir_marts(conn)
    assert tablas["fct_clima_diario"] == 13 and tablas["dim_ciudad"] == 13
    assert tablas["clima_mensual_region"] == 4             # Costa, Sierra, Amazonía, Insular
    assert tablas["indicadores_pais_anual"] == 9
    assert tablas["ecuador_vs_region"] == 1
    with conn.cursor() as cur:
        assert all(r.ok for r in calidad.ejecutar_controles(cur, calidad.controles_marts(), {}, "marts", "x"))
    # reconstruir otra vez da exactamente lo mismo
    assert bd.construir_marts(conn) == tablas
    puesto = contar(conn, "SELECT puesto_ecuador FROM marts.ecuador_vs_region")
    assert 1 <= puesto <= 8


def test_marts_no_se_publican_si_la_validacion_falla(clima_cargado, bd):
    """Write-audit-publish: si un control falla antes del COMMIT, los tableros siguen viendo la versión anterior."""
    conn = clima_cargado
    bd.construir_marts(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE staging.clima_diario SET precipitacion_mm = precipitacion_mm + 100")
    conn.commit()

    def validar(cur):
        calidad.exigir(calidad.evaluar_controles(cur, [calidad.Control(
            "lluvia_imposible", "SELECT count(*) FROM marts.fct_clima_diario WHERE precipitacion_mm > 100")], {}),
            "marts", "x")

    with pytest.raises(calidad.ErrorCalidad, match="lluvia_imposible"):
        bd.construir_marts(conn, validar=validar)
    assert contar(conn, "SELECT max(precipitacion_mm) FROM marts.fct_clima_diario") < 100   # versión anterior
