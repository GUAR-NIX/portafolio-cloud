"""Prueba de integración de punta a punta (necesita el compose levantado; se corre dentro del `cargador`):

    docker compose up -d --wait
    docker compose run --rm cargador pytest -m integracion -p no:cacheprovider

Escribe una muestra limpia en una rama propia de Nessie con PyIceberg, comprueba que `main` no la ve, la fusiona
y la lee desde Trino. Al final borra todo lo que creó.
"""

import os
import uuid
from datetime import datetime
from pathlib import Path

import pytest

pytestmark = pytest.mark.integracion

MUESTRA = Path(__file__).parent / "datos" / "muestra_20260719-19.txt"


@pytest.fixture
def entorno():
    from lakehouse.catalogo import trino
    from lakehouse.nessie import Nessie

    ns = Nessie(os.environ.get("NESSIE_URI", "http://localhost:19120"))
    con = trino()
    if not ns.historial("main", maximo=1):
        # Nessie rechaza el merge ("No common ancestor") si la rama nace de un `main` sin commits.
        cur = con.cursor()
        cur.execute("CREATE SCHEMA iceberg.prueba_primer_commit")
        cur.fetchall()
        cur.execute("DROP SCHEMA iceberg.prueba_primer_commit")
        cur.fetchall()
    rama = f"prueba-{uuid.uuid4().hex[:8]}"
    ns.crear_rama(rama)
    yield ns, rama, con
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS iceberg.prueba_integracion.muestra")
    cur.fetchall()
    cur.execute("DROP SCHEMA IF EXISTS iceberg.prueba_integracion")
    cur.fetchall()
    if rama in ns.ramas():
        ns.borrar_rama(rama)


def test_rama_merge_y_lectura_en_trino(entorno):
    from lakehouse.catalogo import catalogo
    from lakehouse.tablas import tabla_limpia

    ns, rama, con = entorno
    lineas = MUESTRA.read_text(encoding="utf-8").splitlines()
    tabla, res = tabla_limpia(lineas, datetime(2026, 7, 19, 19))
    cat = catalogo(rama)
    cat.create_namespace_if_not_exists("prueba_integracion")
    t = cat.create_table("prueba_integracion.muestra", tabla.schema)
    t.append(tabla)

    cur = con.cursor()
    cur.execute("SHOW SCHEMAS FROM iceberg")
    assert ("prueba_integracion",) not in cur.fetchall()  # main todavía no la ve

    # la rama trae el espacio de nombres y la tabla nuevos
    assert {d["clave"] for d in ns.diferencias("main", rama)} == {"prueba_integracion", "prueba_integracion.muestra"}
    ns.fusionar(rama, "main", "prueba de integración")

    cur.execute("SELECT count(*), sum(vistas) FROM iceberg.prueba_integracion.muestra")
    assert cur.fetchall() == [[res.filas_limpias, res.vistas_limpias]]
