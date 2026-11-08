from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import clima_api.principal as modulo
from clima_api.repositorio import RepositorioClima


@pytest.fixture
def cliente(tmp_path: Path, monkeypatch):
    ruta = tmp_path / "clima.parquet"
    pd.DataFrame(
        [
            {"ciudad":"Quito","fecha":"2024-01-01","temperatura_media_c":14.0,"temperatura_maxima_c":20.0,"temperatura_minima_c":8.0,"precipitacion_mm":2.5},
            {"ciudad":"Quito","fecha":"2024-01-02","temperatura_media_c":15.0,"temperatura_maxima_c":21.0,"temperatura_minima_c":9.0,"precipitacion_mm":0.0},
            {"ciudad":"Cuenca","fecha":"2024-01-01","temperatura_media_c":13.0,"temperatura_maxima_c":19.0,"temperatura_minima_c":7.0,"precipitacion_mm":1.0},
        ]
    ).to_parquet(ruta, index=False)
    monkeypatch.setattr(modulo, "repositorio", RepositorioClima(ruta))
    monkeypatch.setitem(modulo.estado, "drenando", False)
    return TestClient(modulo.app)


def test_salud_no_depende_de_datos(cliente, monkeypatch):
    monkeypatch.setenv("VERSION_APP", "1.0.1")
    assert cliente.get("/salud").json() == {"estado": "vivo", "version": "1.0.1"}


def test_listo_confirma_parquet(cliente):
    assert cliente.get("/listo").status_code == 200


def test_lista_ciudades_ordenadas(cliente):
    assert cliente.get("/ciudades").json() == {"ciudades": ["Cuenca", "Quito"]}


def test_consulta_fecha(cliente):
    respuesta = cliente.get("/clima", params={"ciudad": "quito", "fecha": "2024-01-01"})
    assert respuesta.status_code == 200
    assert respuesta.json()["temperatura_media_c"] == 14.0


def test_clima_devuelve_todas_las_columnas(cliente):
    datos = cliente.get("/clima", params={"ciudad": "Cuenca", "fecha": "2024-01-01"}).json()
    assert datos == {
        "ciudad": "Cuenca",
        "fecha": "2024-01-01",
        "temperatura_media_c": 13.0,
        "temperatura_maxima_c": 19.0,
        "temperatura_minima_c": 7.0,
        "precipitacion_mm": 1.0,
    }


@pytest.mark.parametrize(
    "parametros",
    [
        {"ciudad": "Quito"},
        {"fecha": "2024-01-01"},
        {"ciudad": "Quito", "fecha": "01/01/2024"},
        {"ciudad": "Q", "fecha": "2024-01-01"},
    ],
)
def test_clima_valida_parametros(cliente, parametros):
    assert cliente.get("/clima", params=parametros).status_code == 422


def test_clima_ciudad_inexistente_devuelve_404(cliente):
    respuesta = cliente.get("/clima", params={"ciudad": "Narnia", "fecha": "2024-01-01"})
    assert respuesta.status_code == 404


def test_fecha_inexistente_devuelve_404(cliente):
    assert cliente.get("/clima", params={"ciudad": "Quito", "fecha": "2025-01-01"}).status_code == 404


def test_indicadores(cliente):
    datos = cliente.get("/indicadores/Quito").json()
    assert datos["dias"] == 2
    assert datos["temperatura_media_c"] == 14.5
    assert datos["dias_lluviosos"] == 1


def test_ciudad_inexistente(cliente):
    assert cliente.get("/indicadores/Narnia").status_code == 404


def test_metricas_prometheus(cliente):
    cliente.get("/salud")
    respuesta = cliente.get("/metricas")
    assert respuesta.status_code == 200
    assert "http_requests_total" in respuesta.text


def test_histograma_con_cubetas_finas(cliente):
    cliente.get("/clima", params={"ciudad": "Quito", "fecha": "2024-01-01"})
    texto = cliente.get("/metricas").text
    assert 'http_request_duration_seconds_bucket{handler="/clima",le="0.005"' in texto
    assert 'http_request_duration_seconds_bucket{handler="/clima",le="0.025"' in texto
    assert 'handler="/metricas"' not in texto



def test_drenar_rechaza_clientes_externos(cliente):
    assert cliente.post("/drenar").status_code == 403
    assert "connection" not in cliente.get("/salud").headers


def test_drenaje_cierra_keep_alive_y_saca_de_readiness(cliente):
    local = TestClient(modulo.app, client=("127.0.0.1", 50000))
    assert local.post("/drenar").json() == {"estado": "drenando"}
    respuesta = cliente.get("/clima", params={"ciudad": "Quito", "fecha": "2024-01-01"})
    assert respuesta.status_code == 200
    assert respuesta.headers["connection"] == "close"
    assert cliente.get("/listo").status_code == 503
