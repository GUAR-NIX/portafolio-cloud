"""Los DAG cargan sin errores de importación y respetan las reglas del proyecto. Necesita Airflow instalado
(contenedor `pruebas` o CI); en un Python sin Airflow se omite."""
from pathlib import Path

import pytest

pytest.importorskip("airflow")
try:                                    # Airflow >= 3.2
    from airflow.dag_processing.dagbag import DagBag  # noqa: E402
except ImportError:                     # pragma: no cover
    from airflow.models.dagbag import DagBag  # noqa: E402

CARPETA_DAGS = Path(__file__).resolve().parents[1] / "dags"
ESPERADOS = {"ingesta_clima_ecuador", "ingesta_banco_mundial", "construir_marts"}
POOLS = {"default_pool", "api_open_meteo", "api_banco_mundial"}      # los que crea airflow-init


@pytest.fixture(scope="module")
def dagbag():
    # known_pools: una tarea que pida un pool inexistente es un error de importación
    return DagBag(dag_folder=str(CARPETA_DAGS), known_pools=POOLS)


def test_sin_errores_de_importacion(dagbag):
    assert dagbag.import_errors == {}


def test_estan_los_tres_dags(dagbag):
    assert set(dagbag.dag_ids) == ESPERADOS


@pytest.mark.parametrize("dag_id", sorted(ESPERADOS))
def test_reglas_comunes(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag.tags, "todo DAG lleva etiquetas"
    assert dag.doc_md, "todo DAG lleva documentación"
    assert dag.max_active_runs <= 4
    for t in dag.tasks:
        assert t.execution_timeout is not None, f"{t.task_id} sin execution_timeout"
        assert t.on_failure_callback, f"{t.task_id} sin callback de alerta"
        if t.task_id == "controles_calidad":
            assert t.retries == 0, "los controles de calidad no se reintentan"
        elif not t.task_id.startswith("esperar"):
            assert t.retries >= 3


def test_ingestas_hacen_backfill(dagbag):
    for dag_id in ("ingesta_clima_ecuador", "ingesta_banco_mundial"):
        assert dagbag.get_dag(dag_id).catchup is True


def test_flujo_del_dag_de_clima(dagbag):
    dag = dagbag.get_dag("ingesta_clima_ecuador")
    assert [t.task_id for t in dag.topological_sort()] == ["extraer_a_raw", "cargar_staging", "controles_calidad"]
    assert dag.get_task("extraer_a_raw").pool == "api_open_meteo"
    assert [a.name for a in dag.get_task("controles_calidad").outlets] == ["datos.staging.clima_diario"]


def test_banco_mundial_espera_api_y_mapea_indicadores(dagbag):
    dag = dagbag.get_dag("ingesta_banco_mundial")
    sensor, extraer = dag.get_task("esperar_api"), dag.get_task("extraer_indicador")
    assert sensor.mode == "reschedule"
    assert "extraer_indicador" in sensor.downstream_task_ids
    assert extraer.pool == "api_banco_mundial"
    assert "MappedOperator" in {c.__name__ for c in type(extraer).__mro__}, "una tarea por indicador"


def test_marts_se_disparan_por_assets(dagbag):
    dag = dagbag.get_dag("construir_marts")
    texto = repr(dag.timetable.asset_condition)
    assert "datos.staging.clima_diario" in texto and "datos.staging.indicadores_bm" in texto
    assert dag.max_active_runs == 1


def serializar(dag):
    """Ida y vuelta por el formato serializado: es lo que el dag-processor guarda y el scheduler lee."""
    try:                                    # Airflow >= 3.3
        from airflow.serialization.serialized_objects import DagSerialization as Serializador
    except ImportError:                     # pragma: no cover
        from airflow.serialization.serialized_objects import SerializedDAG as Serializador
    return Serializador.from_dict(Serializador.to_dict(dag))


@pytest.mark.parametrize("dag_id", sorted(ESPERADOS))
def test_dags_se_serializan(dagbag, dag_id):
    assert serializar(dagbag.get_dag(dag_id)).dag_id == dag_id


def test_un_dia_por_ejecucion(dagbag):
    """Cada ejecución del DAG de clima cubre exactamente un día, en hora de Ecuador."""
    import pendulum
    dag = serializar(dagbag.get_dag("ingesta_clima_ecuador"))
    intervalo = dag.timetable.infer_manual_data_interval(
        run_after=pendulum.datetime(2025, 10, 2, 9, tz="America/Guayaquil"))
    assert intervalo.end - intervalo.start == pendulum.duration(days=1)
    assert pendulum.instance(intervalo.start).in_timezone("America/Guayaquil").date().isoformat() == "2025-10-01"
