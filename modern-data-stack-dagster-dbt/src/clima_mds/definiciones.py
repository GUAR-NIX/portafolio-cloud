"""Punto de entrada de Dagster: activos, checks, recursos, jobs y schedules."""

import shutil
import sys
from pathlib import Path

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    RunRequest,
    ScheduleEvaluationContext,
    SkipReason,
    define_asset_job,
    schedule,
)
from dagster_dbt import DbtCliResource

from clima_mds import activos, activos_dbt
from clima_mds import configuracion as cfg


def _ejecutable_dbt() -> str:
    """dbt del PATH o, con el .venv sin activar, el que está junto a este Python."""
    junto_a_python = shutil.which("dbt", path=str(Path(sys.executable).parent))
    return shutil.which("dbt") or junto_a_python or "dbt"


ingesta_anual = define_asset_job(
    "ingesta_anual",
    selection=AssetSelection.assets(activos.ghcn_csv_anual, activos.ghcn_crudo_parquet),
    partitions_def=activos.particiones_anio,
    description="Descarga un año de NOAA y lo publica en Parquet en MinIO (una partición = un año).",
)

transformaciones_dbt = define_asset_job(
    "transformaciones_dbt",
    selection=(
        AssetSelection.assets(activos.ghcn_estaciones, activos.ghcn_paises)
        | AssetSelection.assets(activos_dbt.modelos_dbt)
    ),
    description="Refresca los metadatos de estaciones y ejecuta dbt build (modelos + pruebas).",
)


@schedule(cron_schedule="0 6 * * *", job=ingesta_anual, execution_timezone="America/Guayaquil",
          default_status=DefaultScheduleStatus.STOPPED)
def ingesta_diaria_anio_en_curso(context: ScheduleEvaluationContext):
    """Cada día re-descarga el año en curso (NOAA lo actualiza a diario); los años cerrados no cambian."""
    anio = str(context.scheduled_execution_time.year)
    if anio not in cfg.ANIOS:
        return SkipReason(f"El año {anio} no está entre las particiones {cfg.ANIOS}")
    return RunRequest(run_key=context.scheduled_execution_time.date().isoformat(), partition_key=anio)


transformaciones_diarias = schedule(
    cron_schedule="0 7 * * *", job=transformaciones_dbt, execution_timezone="America/Guayaquil",
    default_status=DefaultScheduleStatus.STOPPED, name="transformaciones_diarias",
)(lambda _context: RunRequest())

defs = Definitions(
    assets=[activos.ghcn_csv_anual, activos.ghcn_crudo_parquet, activos.ghcn_estaciones, activos.ghcn_paises,
            activos_dbt.modelos_dbt],
    asset_checks=[activos_dbt.mart_pais_plausible],
    jobs=[ingesta_anual, transformaciones_dbt],
    schedules=[ingesta_diaria_anio_en_curso, transformaciones_diarias],
    resources={"dbt": DbtCliResource(project_dir=activos_dbt.proyecto_dbt, dbt_executable=_ejecutable_dbt())},
)
