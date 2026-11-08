"""Pruebas de la interfaz Streamlit con la API simulada (sin red)."""

from pathlib import Path
from unittest.mock import patch

import pytest
import requests

AppTest = pytest.importorskip("streamlit.testing.v1").AppTest

RUTA_APP = str(Path(__file__).resolve().parents[1] / "app" / "app.py")
INDICADORES = {
    "Cuenca": {
        "ciudad": "Cuenca",
        "temperatura_media_c": 14.1,
        "temperatura_maxima_c": 24.3,
        "precipitacion_total_mm": 900.5,
        "dias_lluviosos": 150,
    },
    "Quito": {
        "ciudad": "Quito",
        "temperatura_media_c": 13.9,
        "temperatura_maxima_c": 23.0,
        "precipitacion_total_mm": 1200.0,
        "dias_lluviosos": 200,
    },
}


class RespuestaFalsa:
    def __init__(self, cuerpo: dict, estado: int = 200):
        self.cuerpo = cuerpo
        self.status_code = estado

    def json(self) -> dict:
        return self.cuerpo

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


def api_falsa(url: str, timeout: int) -> RespuestaFalsa:
    assert timeout == 5
    if url.endswith("/ciudades"):
        return RespuestaFalsa({"ciudades": sorted(INDICADORES)})
    ciudad = url.rsplit("/", 1)[-1]
    if ciudad in INDICADORES:
        return RespuestaFalsa(INDICADORES[ciudad])
    return RespuestaFalsa({"detail": "Ciudad no encontrada"}, 404)


def ejecutar(**kwargs) -> AppTest:
    with patch("requests.get", **kwargs):
        prueba = AppTest.from_file(RUTA_APP, default_timeout=30)
        prueba.run()
    return prueba


def test_muestra_metricas_de_la_primera_ciudad():
    prueba = ejecutar(side_effect=api_falsa)
    assert not prueba.exception
    assert prueba.selectbox[0].options == ["Cuenca", "Quito"]
    valores = {m.label: m.value for m in prueba.metric}
    assert valores["Temperatura media"] == "14.1 °C"
    assert valores["Precipitación"] == "900.5 mm"
    assert valores["Días lluviosos"] == "150"


def test_cambiar_ciudad_consulta_la_api():
    with patch("requests.get", side_effect=api_falsa) as simulado:
        prueba = AppTest.from_file(RUTA_APP, default_timeout=30)
        prueba.run()
        prueba.selectbox[0].select("Quito").run()
    assert {m.label: m.value for m in prueba.metric}["Máxima"] == "23.0 °C"
    assert simulado.call_args_list[-1].args[0].endswith("/indicadores/Quito")


def test_api_caida_muestra_error_sin_excepcion():
    prueba = ejecutar(side_effect=requests.ConnectionError("rechazada"))
    assert not prueba.exception
    assert "La API no está disponible" in prueba.error[0].value
