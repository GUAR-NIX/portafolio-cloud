"""Resultados de dbt build sobre la muestra versionada (target ci). Se omiten si no se corrió dbt."""

import duckdb
import pytest
from conftest import RAIZ

BASE = RAIZ / "dbt_clima" / "target" / "ci.duckdb"
pytestmark = pytest.mark.skipif(not BASE.exists(), reason="Correr antes: dbt build --target ci")


@pytest.fixture(scope="module")
def con():
    with duckdb.connect(str(BASE), read_only=True) as c:
        try:  # las vistas guardan la ruta absoluta de los Parquet: la base solo sirve donde se construyó
            c.sql("select 1 from staging.stg_ghcn__observaciones limit 1").fetchone()
        except duckdb.IOException:
            pytest.skip("ci.duckdb se construyó en otro entorno (host o contenedor): vuelve a correr dbt build")
        yield c


def test_unidades_convertidas(con):
    # décimas de °C → °C: ninguna TMAX válida supera 60 °C
    assert con.sql("select max(tmax_c) from intermediate.int_observaciones_diarias").fetchone()[0] < 60


def test_qflag_descarta_valores(con):
    crudas, descartadas = con.sql(
        """select count(*), count(*) filter (where bandera_calidad is not null)
           from staging.stg_ghcn__observaciones"""
    ).fetchone()
    assert crudas > 0
    total = con.sql("select sum(valores_descartados_qflag) from intermediate.int_observaciones_diarias").fetchone()[0]
    assert total == descartadas


def test_pivot_una_fila_por_estacion_y_dia(con):
    filas, distintas = con.sql(
        "select count(*), count(distinct (id_estacion, fecha)) from intermediate.int_observaciones_diarias"
    ).fetchone()
    assert filas == distintas > 0


def test_anomalias_fuera_del_periodo_normal(con):
    # la normal es 2022-2024: hay anomalías calculadas para años posteriores
    n = con.sql(
        "select count(*) from marts.mart_clima_mensual_pais where anio >= 2025 and anomalia_tmedia_c is not null"
    ).fetchone()[0]
    assert n > 0


def test_extremos_coherentes(con):
    malos = con.sql("select count(*) from marts.mart_extremos_pais_anual where tmax_max_c < tmin_min_c").fetchone()[0]
    assert malos == 0
