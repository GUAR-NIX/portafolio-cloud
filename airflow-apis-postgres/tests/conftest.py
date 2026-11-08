import json
import os
import sys
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"
sys.path.insert(0, str(RAIZ / "include"))


def cargar_fixture(nombre: str):
    return json.loads((FIXTURES / nombre).read_text(encoding="utf-8"))


@pytest.fixture
def respuesta_clima():
    """Respuesta real de Open-Meteo (archivo ERA5) para las 13 ciudades el 15-oct-2025."""
    return cargar_fixture("open_meteo_2025-10-15.json")


@pytest.fixture
def respuesta_bm():
    """Respuesta real del Banco Mundial: PIB per cápita 2023, 9 países/agregados."""
    return cargar_fixture("banco_mundial_NY.GDP.PCAP.CD_2023.json")


@pytest.fixture
def conn():
    """Conexión a una base de pruebas vacía (DATOS_PRUEBAS_URI). Sin ella, las pruebas de SQL se omiten."""
    uri = os.environ.get("DATOS_PRUEBAS_URI")
    if not uri:
        pytest.skip("DATOS_PRUEBAS_URI no definido: corre `docker compose run --rm pruebas` o la CI")
    psycopg2 = pytest.importorskip("psycopg2")
    c = psycopg2.connect(uri)
    with c.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS raw, staging, marts, auditoria CASCADE")
    c.commit()
    from ingesta import bd
    bd.asegurar_esquemas(c)
    yield c
    c.close()
