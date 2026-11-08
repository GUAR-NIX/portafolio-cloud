"""Modelos dbt como activos de Dagster (dagster-dbt): cada modelo es un activo y cada prueba un asset check."""

import json
from collections import Counter

import duckdb
from dagster import AssetCheckResult, AssetCheckSeverity, AssetExecutionContext, AssetKey, asset_check
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from clima_mds import configuracion as cfg

proyecto_dbt = DbtProject(project_dir=cfg.DIR_DBT, profiles_dir=cfg.DIR_DBT)
GRUPOS = {"staging": "dbt_staging", "intermediate": "dbt_intermediate", "marts": "dbt_marts"}


class TraductorClima(DagsterDbtTranslator):
    """Agrupa los activos por capa de dbt (carpeta) para que el grafo se lea por capas."""

    def get_group_name(self, dbt_resource_props):
        fqn = dbt_resource_props.get("fqn", [])
        capa = fqn[1] if len(fqn) > 2 else None
        return GRUPOS.get(capa) or super().get_group_name(dbt_resource_props)


def resumir_run_results(run_results: dict) -> dict:
    """Cuenta modelos y pruebas por estado a partir de run_results.json."""
    conteo = Counter()
    for r in run_results.get("results", []):
        tipo = r["unique_id"].split(".")[0]
        conteo[f"{tipo}:{r['status']}"] += 1
    return {
        "generado": run_results.get("metadata", {}).get("generated_at"),
        "segundos_totales": round(run_results.get("elapsed_time", 0), 1),
        "por_tipo_y_estado": dict(sorted(conteo.items())),
        "nodos": [
            {"id": r["unique_id"], "estado": r["status"], "segundos": round(r.get("execution_time", 0), 2),
             "fallos": r.get("failures")}
            for r in run_results.get("results", [])
        ],
    }


@dbt_assets(manifest=proyecto_dbt.manifest_path, project=proyecto_dbt, dagster_dbt_translator=TraductorClima())
def modelos_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    invocacion = dbt.cli(["build"], context=context)
    yield from invocacion.stream()
    resumen = resumir_run_results(invocacion.get_artifact("run_results.json"))
    destino = cfg.DIR_REPORTES / "dbt" / "build_resumen.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(resumen, indent=2, ensure_ascii=False), encoding="utf-8")


@asset_check(asset=AssetKey(["marts", "mart_clima_mensual_pais"]), blocking=True,
             description="Cobertura de países y anomalías plausibles en el mart publicado (control de negocio).")
def mart_pais_plausible() -> AssetCheckResult:
    with duckdb.connect(cfg.RUTA_DUCKDB, read_only=True) as con:
        paises, absurdas = con.execute(
            """SELECT count(DISTINCT codigo_pais),
                      count(*) FILTER (WHERE abs(anomalia_tmedia_c) > 15)
               FROM marts.mart_clima_mensual_pais"""
        ).fetchone()
    return AssetCheckResult(
        passed=paises >= 100 and absurdas == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"paises": paises, "meses_pais_con_anomalia_mayor_a_15C": absurdas},
    )
