"""Definiciones de Dagster: linaje Python → dbt, jobs, schedule y asset check bloqueante (sin MinIO ni red)."""

import gzip

import pytest
from conftest import RAIZ, escribir_csv_gz

MANIFIESTO = RAIZ / "dbt_clima" / "target" / "manifest.json"
pytestmark = pytest.mark.skipif(not MANIFIESTO.exists(), reason="Falta dbt parse (manifest.json)")


@pytest.fixture(scope="module")
def defs():
    from clima_mds.definiciones import defs

    return defs


def test_definiciones_cargan(defs):
    from dagster import Definitions

    Definitions.validate_loadable(defs)
    assert {j.name for j in defs.resolve_all_job_defs()} >= {"ingesta_anual", "transformaciones_dbt"}


def test_linaje_python_a_dbt(defs):
    from dagster import AssetKey

    grafo = defs.resolve_asset_graph()
    padres = grafo.get(AssetKey(["staging", "stg_ghcn__observaciones"])).parent_keys
    assert AssetKey(["ghcn", "ghcn_crudo_parquet"]) in padres
    marts = {k for k in grafo.get_all_asset_keys() if k.path[-1].startswith("mart_")}
    assert len(marts) == 3
    assert grafo.get(AssetKey(["marts", "mart_clima_mensual_pais"])).group_name == "dbt_marts"
    # las pruebas de dbt aparecen como asset checks de Dagster
    assert len(grafo.asset_check_keys) >= 30


def test_schedule_diario_pide_el_anio_en_curso():
    from datetime import datetime

    from dagster import RunRequest, SkipReason, build_schedule_context

    from clima_mds.definiciones import ingesta_diaria_anio_en_curso as sched

    peticion = sched(build_schedule_context(scheduled_execution_time=datetime(2026, 10, 25, 6)))
    assert isinstance(peticion, RunRequest) and peticion.partition_key == "2026"
    assert isinstance(sched(build_schedule_context(scheduled_execution_time=datetime(2030, 1, 1, 6))), SkipReason)


def test_activo_crudo_bloquea_con_datos_malos(tmp_path, csv_valido, monkeypatch):
    from dagster import AssetKey, materialize

    from clima_mds import activos, almacen, calidad
    from clima_mds import configuracion as cfg

    (tmp_path / "by_year").mkdir()
    csv_valido.rename(tmp_path / "by_year" / "2024.csv.gz")
    monkeypatch.setattr(cfg, "DIR_CRUDO", tmp_path)
    monkeypatch.setattr(cfg, "DIR_REPORTES", tmp_path / "reports")
    monkeypatch.setattr(cfg, "ruta_s3_crudo", lambda anio: (tmp_path / f"{anio}.parquet").as_posix())
    conectar_local = almacen.conectar
    monkeypatch.setattr(almacen, "conectar", lambda: conectar_local(s3=False))
    monkeypatch.setattr(almacen, "tamano_objeto_s3", lambda con, ruta: 1)
    monkeypatch.setattr(calidad, "FILAS_MINIMAS", 100)

    ok = materialize([activos.ghcn_crudo_parquet], partition_key="2024", raise_on_error=False)
    assert ok.success
    assert all(e.passed for e in ok.get_asset_check_evaluations())

    # TMAX absurda sin bandera: el check bloqueante falla y la corrida queda en rojo
    ruta = tmp_path / "by_year" / "2024.csv.gz"
    filas = gzip.decompress(ruta.read_bytes()).decode().splitlines()
    escribir_csv_gz(ruta, filas + ["EC000084370,20240301,TMAX,9999,,,S,"])
    malo = materialize([activos.ghcn_crudo_parquet], partition_key="2024", raise_on_error=False)
    fallidos = [e.check_name for e in malo.get_asset_check_evaluations() if not e.passed]
    assert fallidos == ["temperatura_en_rango_fisico"]
    assert not malo.success
    assert malo.get_asset_check_evaluations()[0].asset_key == AssetKey(["ghcn", "ghcn_crudo_parquet"])
