import json
from pathlib import Path

import pytest

from serverless_sismos.geografia import distancia_a_ecuador_km, distancia_haversine_km
from serverless_sismos.modelos import Sismo
from serverless_sismos.procesamiento import evaluar_alerta, procesar_geojson


@pytest.fixture
def documento():
    ruta = Path(__file__).parent / "fixtures/sismos.geojson"
    return json.loads(ruta.read_text(encoding="utf-8"))


def test_haversine_mismo_punto_es_cero():
    assert distancia_haversine_km(-1.8, -78.1, -1.8, -78.1) == pytest.approx(0)


def test_distancia_quito_es_cercana():
    assert distancia_a_ecuador_km(-0.18, -78.47) < 250


def test_modelo_normaliza_item(documento):
    sismo = Sismo.model_validate(documento["features"][0])
    assert sismo.como_item()["id_evento"] == "ec-fuerte"
    assert sismo.como_item()["magnitud"] == "5.6"


def test_alerta_fuerte_y_cercano(documento):
    sismo = Sismo.model_validate(documento["features"][0])
    assert evaluar_alerta(sismo) is not None


def test_sin_alerta_si_es_suave(documento):
    sismo = Sismo.model_validate(documento["features"][1])
    assert evaluar_alerta(sismo) is None


def test_sin_alerta_si_es_lejano(documento):
    sismo = Sismo.model_validate(documento["features"][2])
    assert evaluar_alerta(sismo) is None


def test_procesamiento_cuenta_validos_invalidos_y_alertas(documento):
    almacen = {}
    alertas = []

    def guardar(item):
        if item["id_evento"] in almacen:
            return False
        almacen[item["id_evento"]] = item
        return True

    resultado = procesar_geojson(documento, guardar, alertas.append)
    assert resultado == {
        "recibidos": 4,
        "validos": 3,
        "invalidos": 1,
        "insertados": 3,
        "duplicados": 0,
        "alertas": 1,
        "errores": resultado["errores"],
    }
    assert len(resultado["errores"]) == 1


def test_repeticion_es_idempotente(documento):
    ids = set()

    def guardar(item):
        if item["id_evento"] in ids:
            return False
        ids.add(item["id_evento"])
        return True

    procesar_geojson(documento, guardar, lambda _mensaje: None)
    segunda = procesar_geojson(documento, guardar, lambda _mensaje: None)
    assert segunda["insertados"] == 0
    assert segunda["duplicados"] == 3


@pytest.mark.parametrize("documento", [{}, {"type": "FeatureCollection", "features": {}}])
def test_rechaza_documento_que_no_es_coleccion(documento):
    with pytest.raises(ValueError):
        procesar_geojson(documento, lambda _: True, lambda _: None)



def test_errores_de_validadores_propios_se_pueden_serializar(documento):
    """La Lambda escribe los errores en S3: deben convertirse a JSON sin fallar."""
    evento = json.loads(json.dumps(documento["features"][0]))
    evento["geometry"]["coordinates"][1] = 120.0
    lote = {"type": "FeatureCollection", "features": [evento]}
    resultado = procesar_geojson(lote, lambda _item: True, lambda _alerta: None)
    texto = json.dumps(resultado["errores"], ensure_ascii=False)
    assert "Coordenadas fuera de rango" in texto
