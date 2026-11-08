"""Lógica pura: selección de API, parseo de JSON, reintentos HTTP y reglas de calidad (sin red ni base)."""
import copy
from datetime import date
from pathlib import Path

import pytest
import requests

from ingesta import apis, calidad, config
from ingesta.transformar import ErrorFormato, filas_banco_mundial, filas_clima

DIA = date(2025, 10, 15)


# ------------------------------------------------------------------------------------------------- clima
def test_filas_clima_una_por_ciudad_con_columnas_mapeadas(respuesta_clima):
    filas = filas_clima(respuesta_clima, DIA, "archivo")
    assert [f["ciudad"] for f in filas] == [c["ciudad"] for c in config.CIUDADES]
    quito, guayaquil = filas[0], filas[1]
    assert set(config.VARIABLES_CLIMA.values()) <= set(quito)
    assert quito["elevacion_m"] > 2500 and guayaquil["elevacion_m"] < 50
    assert guayaquil["temp_media_c"] - quito["temp_media_c"] > 5
    assert all(f["fecha"] == DIA and f["fuente"] == "archivo" for f in filas)


def test_respuesta_de_una_sola_ubicacion_es_un_objeto(respuesta_clima):
    filas = filas_clima(respuesta_clima[0], DIA, "archivo", ciudades=config.CIUDADES[:1])
    assert len(filas) == 1 and filas[0]["ciudad"] == "Quito"


def test_falla_si_faltan_ubicaciones(respuesta_clima):
    with pytest.raises(ErrorFormato, match="13 ubicaciones"):
        filas_clima(respuesta_clima[:12], DIA, "archivo")


def test_falla_si_la_respuesta_viene_en_otro_orden(respuesta_clima):
    cruzada = copy.deepcopy(respuesta_clima)
    cruzada[0], cruzada[1] = cruzada[1], cruzada[0]
    with pytest.raises(ErrorFormato, match="no corresponde a Quito"):
        filas_clima(cruzada, DIA, "archivo")


def test_falla_si_no_trae_el_dia_pedido(respuesta_clima):
    with pytest.raises(ErrorFormato, match="no trae el día"):
        filas_clima(respuesta_clima, date(2025, 10, 16), "archivo")


def test_falla_si_falta_una_variable(respuesta_clima):
    sin_lluvia = copy.deepcopy(respuesta_clima)
    del sin_lluvia[3]["daily"]["precipitation_sum"]
    with pytest.raises(ErrorFormato, match="precipitation_sum"):
        filas_clima(sin_lluvia, DIA, "archivo")


@pytest.mark.parametrize("fecha, esperado", [
    (date(2026, 10, 18), "pronostico"),    # ayer
    (date(2026, 10, 14), "pronostico"),    # 5 días: todavía provisional
    (date(2026, 10, 13), "archivo"),       # 6 días: ya es definitivo
    (date(2025, 10, 1), "archivo"),
])
def test_elegir_fuente_clima(fecha, esperado):
    assert apis.elegir_fuente_clima(fecha, hoy=date(2026, 10, 19)) == esperado


def test_no_se_piden_fechas_futuras():
    with pytest.raises(ValueError):
        apis.elegir_fuente_clima(date(2026, 10, 20), hoy=date(2026, 10, 19))


def test_parametros_clima_respetan_la_cuota():
    p = apis.parametros_clima(DIA)
    assert len(p["daily"].split(",")) < 10          # con 10 o más variables cada ciudad cuenta doble
    assert len(p["latitude"].split(",")) == len(config.CIUDADES)
    assert p["start_date"] == p["end_date"] == "2025-10-15"
    # 6 peticiones/min × 13 ciudades × 60 min debe quedar bajo el límite de 5.000 llamadas por hora
    assert 3600 / config.SEGUNDOS_ENTRE_LLAMADAS * len(config.CIUDADES) < 5000


# ----------------------------------------------------------------------------------------- Banco Mundial
def test_filas_banco_mundial(respuesta_bm):
    filas = filas_banco_mundial(respuesta_bm, "NY.GDP.PCAP.CD", 2023)
    paises = {f["pais_iso3"] for f in filas}
    assert {"ECU", "COL", "PER", "LCN"} <= paises and paises <= set(config.PAISES)
    ecuador = next(f for f in filas if f["pais_iso3"] == "ECU")
    assert 5000 < ecuador["valor"] < 9000


def test_banco_mundial_descarta_nulos(respuesta_bm):
    con_nulo = copy.deepcopy(respuesta_bm)
    con_nulo[0]["value"] = None
    assert len(filas_banco_mundial(con_nulo, "NY.GDP.PCAP.CD", 2023)) == len(respuesta_bm) - 1


@pytest.mark.parametrize("indicador, anio, mensaje", [("SP.POP.TOTL", 2023, "indicador"),
                                                      ("NY.GDP.PCAP.CD", 2022, "año")])
def test_banco_mundial_rechaza_datos_de_otra_particion(respuesta_bm, indicador, anio, mensaje):
    with pytest.raises(ErrorFormato, match=mensaje):
        filas_banco_mundial(respuesta_bm, indicador, anio)


# ---------------------------------------------------------------------------------------- reintentos HTTP
class RespuestaFalsa:
    def __init__(self, estado, cuerpo=None, cabeceras=None):
        self.status_code, self._cuerpo, self.headers = estado, cuerpo, cabeceras or {}
        self.text, self.url = str(cuerpo), "https://api.prueba"

    def json(self):
        return self._cuerpo


class SesionFalsa:
    def __init__(self, respuestas):
        self.respuestas, self.llamadas = list(respuestas), 0

    def get(self, *a, **k):
        self.llamadas += 1
        r = self.respuestas.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


@pytest.fixture
def sin_esperas(monkeypatch):
    esperas = []
    monkeypatch.setattr(apis.time, "sleep", esperas.append)
    return esperas


def test_reintenta_errores_transitorios_con_espera_exponencial(sin_esperas):
    s = SesionFalsa([RespuestaFalsa(503), requests.Timeout("lento"), RespuestaFalsa(200, {"ok": 1})])
    assert apis.pedir_json("u", {}, sesion=s) == {"ok": 1}
    assert s.llamadas == 3 and sin_esperas == [2.0, 4.0]


def test_respeta_retry_after_en_429(sin_esperas):
    s = SesionFalsa([RespuestaFalsa(429, cabeceras={"Retry-After": "7"}), RespuestaFalsa(200, [1])])
    assert apis.pedir_json("u", {}, sesion=s) == [1]
    assert sin_esperas == [7.0]


def test_error_400_no_se_reintenta(sin_esperas):
    s = SesionFalsa([RespuestaFalsa(400, {"reason": "variable desconocida"})])
    with pytest.raises(apis.ErrorApi, match="HTTP 400"):
        apis.pedir_json("u", {}, sesion=s)
    assert s.llamadas == 1 and sin_esperas == []


def test_se_rinde_despues_del_ultimo_intento(sin_esperas):
    s = SesionFalsa([RespuestaFalsa(500)] * 3)
    with pytest.raises(apis.ErrorApi, match="HTTP 500"):
        apis.pedir_json("u", {}, intentos=3, sesion=s)
    assert s.llamadas == 3


def test_banco_mundial_error_dentro_de_http_200(sin_esperas):
    s = SesionFalsa([RespuestaFalsa(200, [{"message": [{"key": "Invalid value"}]}])])
    with pytest.raises(apis.ErrorApi, match="Respuesta inesperada"):
        apis.descargar_indicador("NO.EXISTE", 2023, sesion=s)


def test_banco_mundial_recorre_todas_las_paginas(sin_esperas):
    fila = {"indicator": {"id": "X"}, "date": "2023", "value": 1}
    s = SesionFalsa([RespuestaFalsa(200, [{"pages": 2}, [fila]]), RespuestaFalsa(200, [{"pages": 2}, [fila]])])
    assert len(apis.descargar_indicador("X", 2023, sesion=s)) == 2


# ------------------------------------------------------------------------------------------ reglas de calidad
@pytest.mark.parametrize("valor, minimo, maximo, ok", [
    (0, 0, 0, True), (1, 0, 0, False), (13, 13, 13, True), (5, 3, None, True), (2, 3, None, False),
    (None, 0, 0, False), (-1, None, 10, True),
])
def test_dentro_de_rango(valor, minimo, maximo, ok):
    assert calidad.dentro_de_rango(valor, minimo, maximo) is ok


def test_hay_un_control_de_rango_por_indicador():
    nombres = {c.nombre for c in calidad.controles_banco_mundial()}
    assert {f"rango_{col}" for col, _, _ in config.INDICADORES.values()} <= nombres


def test_mart_ancho_tiene_una_columna_por_indicador():
    sql = (Path(__file__).parents[1] / "sql" / "marts" / "06_indicadores_pais_anual.sql").read_text(encoding="utf-8")
    for codigo, (columna, _, _) in config.INDICADORES.items():
        assert f"indicador = '{codigo}')" in sql and f"AS {columna}" in sql
