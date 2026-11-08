"""Mediciones (filas por capa, tamaños, tiempos, pruebas dbt) y consultas de negocio con figuras.

Lee el almacén DuckDB (solo lectura), el Parquet de MinIO, los JSON de reports/ y la instancia de Dagster.
Uso (con el stack levantado y después de dbt build):
    docker compose exec dagster-daemon python scripts/analizar.py
Salidas: reports/metricas.json, reports/consultas_negocio.json, reports/*.csv y reports/figures/*.png
"""

import json
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from clima_mds import almacen  # noqa: E402
from clima_mds import configuracion as cfg  # noqa: E402

REP = cfg.DIR_REPORTES
FIG = REP / "figures"
# Paleta de referencia (dataviz): categóricos en orden fijo y par divergente azul ↔ rojo
AZUL, NARANJA, AQUA, ROJO = "#2a78d6", "#eb6834", "#1baf7a", "#e34948"
TINTA, TINTA_2, REJILLA, FONDO = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
ANIO_ANALISIS = 2025
MIN_ESTACIONES = 3
MIN_MESES = 10

plt.rcParams.update({
    "figure.facecolor": FONDO, "axes.facecolor": FONDO, "savefig.facecolor": FONDO,
    "axes.edgecolor": REJILLA, "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA_2,
    "text.color": TINTA, "axes.grid": True, "grid.color": REJILLA, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 10, "axes.titlesize": 12,
    "axes.titleweight": "bold", "axes.titlelocation": "left",
})


def leer_json(ruta: Path) -> dict:
    return json.loads(ruta.read_text(encoding="utf-8"))


def guardar(fig, nombre: str) -> None:
    fig.tight_layout()
    fig.savefig(FIG / nombre, dpi=130)
    plt.close(fig)


def cronometrar(con, sql: str) -> float:
    t0 = time.perf_counter()
    con.execute(sql).fetchall()
    return round(time.perf_counter() - t0, 2)


def duracion_corridas() -> list[dict]:
    """Duración real de cada corrida del backfill (instancia de Dagster)."""
    from dagster import DagsterInstance, DagsterRunStatus, RunsFilter

    instancia = DagsterInstance.get()
    registros = instancia.get_run_records(RunsFilter(job_name="ingesta_anual", statuses=[DagsterRunStatus.SUCCESS]))
    corridas = {}
    for r in sorted(registros, key=lambda r: r.start_time or 0):
        anio = r.dagster_run.tags.get("dagster/partition")
        if anio and r.start_time and r.end_time:
            corridas[anio] = {"anio": anio, "run_id": r.dagster_run.run_id[:8],
                              "segundos_corrida": round(r.end_time - r.start_time, 1)}
    return [corridas[a] for a in sorted(corridas)]


def main() -> None:
    FIG.mkdir(parents=True, exist_ok=True)
    con = almacen.conectar()
    con.execute(f"ATTACH '{cfg.RUTA_DUCKDB}' AS dw (READ_ONLY)")

    # ---------------- capas ----------------
    por_anio = []
    for anio in cfg.ANIOS:
        d = leer_json(REP / "ingesta" / f"descarga_{anio}.json")
        c = leer_json(REP / "ingesta" / f"crudo_{anio}.json")
        por_anio.append({
            "anio": anio, "filas_crudas": c["filas"], "estaciones": c["estaciones"],
            "MB_csv": round(d["bytes_csv_descomprimido"] / 1e6, 1), "MB_csv_gz": round(d["bytes_csv_gz"] / 1e6, 1),
            "MB_parquet": round(c["bytes_parquet"] / 1e6, 1),
            "segundos_descarga": d["segundos_descarga"], "segundos_conversion": c["segundos_conversion"],
            "segundos_controles": c["segundos_controles"],
        })
    for fila, corrida in zip(por_anio, duracion_corridas(), strict=False):
        if corrida["anio"] == fila["anio"]:
            fila.update(corrida)

    tablas = ["staging.stg_ghcn__observaciones", "staging.stg_ghcn__estaciones", "staging.stg_ghcn__paises",
              "intermediate.int_observaciones_diarias", "intermediate.int_estaciones",
              "intermediate.int_clima_mensual_estacion", "intermediate.int_normal_mensual_estacion",
              "marts.mart_clima_mensual_estacion", "marts.mart_clima_mensual_pais", "marts.mart_extremos_pais_anual"]
    filas_capa = {"crudo.ghcn_crudo_parquet": sum(f["filas_crudas"] for f in por_anio)}
    for t in tablas:
        filas_capa[t] = con.execute(f"SELECT count(*) FROM dw.{t}").fetchone()[0]

    descartadas = con.execute(
        "SELECT sum(valores_descartados_qflag) FROM dw.intermediate.int_observaciones_diarias").fetchone()[0]

    # consulta de referencia: mismo motor, CSV comprimido frente a Parquet
    sql_ref = ("SELECT fecha // 100 % 100 AS mes, avg(valor) FROM {f} WHERE elemento = 'TMAX' AND q_flag IS NULL "
               "GROUP BY 1")
    t_csv = cronometrar(con, sql_ref.format(f=almacen.sql_leer_csv(
        (cfg.DIR_CRUDO / "by_year" / f"{ANIO_ANALISIS}.csv.gz").as_posix())))
    t_parquet = cronometrar(con, sql_ref.format(f=f"read_parquet('{cfg.ruta_s3_crudo(str(ANIO_ANALISIS))}')"))

    dbt = leer_json(REP / "dbt" / "build_resumen.json")
    tam_duckdb = Path(cfg.RUTA_DUCKDB).stat().st_size
    metricas = {
        "por_anio": por_anio,
        "totales": {
            "filas_crudas": filas_capa["crudo.ghcn_crudo_parquet"],
            "MB_csv": round(sum(f["MB_csv"] for f in por_anio), 1),
            "MB_csv_gz": round(sum(f["MB_csv_gz"] for f in por_anio), 1),
            "MB_parquet": round(sum(f["MB_parquet"] for f in por_anio), 1),
            "MB_duckdb": round(tam_duckdb / 1e6, 1),
        },
        "filas_por_capa": filas_capa,
        "valores_descartados_por_qflag": int(descartadas),
        "consulta_referencia_tmax_mensual": {"anio": ANIO_ANALISIS, "segundos_csv_gz": t_csv,
                                             "segundos_parquet_minio": t_parquet},
        "dbt_build": {"segundos": dbt["segundos_totales"], "conteo": dbt["por_tipo_y_estado"]},
    }
    (REP / "metricas.json").write_text(json.dumps(metricas, indent=2, ensure_ascii=False), encoding="utf-8")

    # ---------------- negocio ----------------
    anual = con.execute(f"""
        SELECT codigo_pais, any_value(pais) AS pais, avg(anomalia_tmedia_c) AS anomalia_c,
               count(anomalia_tmedia_c) AS meses, round(avg(estaciones_con_anomalia)) AS estaciones
        FROM dw.marts.mart_clima_mensual_pais
        WHERE anio = {ANIO_ANALISIS}
        GROUP BY codigo_pais
        HAVING count(anomalia_tmedia_c) >= {MIN_MESES}
           AND min(estaciones_con_anomalia) FILTER (WHERE anomalia_tmedia_c IS NOT NULL) >= {MIN_ESTACIONES}
        ORDER BY anomalia_c DESC""").df()
    anual.to_csv(REP / f"anomalia_pais_{ANIO_ANALISIS}.csv", index=False)

    mensual = con.execute("""
        SELECT mes_inicio, avg(anomalia_tmedia_c) AS anomalia_estaciones, count(anomalia_tmedia_c) AS n
        FROM dw.marts.mart_clima_mensual_estacion GROUP BY 1 HAVING count(anomalia_tmedia_c) > 1000
        ORDER BY 1""").df()
    mensual_pais = con.execute(f"""
        SELECT mes_inicio, avg(anomalia_tmedia_c) AS anomalia_paises
        FROM dw.marts.mart_clima_mensual_pais WHERE estaciones_con_anomalia >= {MIN_ESTACIONES}
        GROUP BY 1 ORDER BY 1""").df()
    mensual = mensual.merge(mensual_pais, on="mes_inicio")
    mensual.to_csv(REP / "anomalia_mensual_global.csv", index=False)

    extremos = con.execute(f"""
        SELECT pais, tmax_max_c, estacion_tmax_max, fecha_tmax_max, tmin_min_c, estacion_tmin_min, fecha_tmin_min,
               prcp_max_diaria_mm, estacion_prcp_max, fecha_prcp_max, estaciones
        FROM dw.marts.mart_extremos_pais_anual WHERE anio = {ANIO_ANALISIS} AND estaciones >= {MIN_ESTACIONES}
        ORDER BY tmax_max_c DESC""").df()
    extremos.to_csv(REP / f"extremos_pais_{ANIO_ANALISIS}.csv", index=False)

    # Línea base ingenua: media de TODAS las estaciones que reportaron en cada periodo (la red cambia)
    linea_base = con.execute(f"""
        WITH m AS (SELECT * FROM dw.marts.mart_clima_mensual_estacion WHERE mes_completo_temp),
        ingenua AS (
            SELECT codigo_pais,
                   avg(tmedia_c) FILTER (WHERE anio = {ANIO_ANALISIS}) -
                   avg(tmedia_c) FILTER (WHERE anio BETWEEN 2022 AND 2024) AS anomalia_ingenua_c
            FROM m GROUP BY codigo_pais)
        SELECT a.codigo_pais, any_value(a.pais) AS pais, avg(a.anomalia_tmedia_c) AS anomalia_estaciones_c,
               any_value(i.anomalia_ingenua_c) AS anomalia_ingenua_c,
               round(avg(a.estaciones_con_anomalia)) AS estaciones
        FROM dw.marts.mart_clima_mensual_pais a JOIN ingenua i USING (codigo_pais)
        WHERE a.anio = {ANIO_ANALISIS}
        GROUP BY a.codigo_pais
        HAVING count(a.anomalia_tmedia_c) = 12 AND min(a.estaciones_con_anomalia) >= 20""").df()
    linea_base["diferencia_abs_c"] = (linea_base.anomalia_ingenua_c - linea_base.anomalia_estaciones_c).abs()
    linea_base.sort_values("diferencia_abs_c", ascending=False).to_csv(REP / "linea_base_ingenua.csv", index=False)

    # Cobertura de Ecuador en GHCN (¿podría el servicio publicar indicadores del país con esta fuente?)
    ecuador = con.execute("""
        SELECT anio, count(DISTINCT id_estacion) AS estaciones, count(tmax_c) AS dias_estacion_tmax,
               count(tmin_c) AS dias_estacion_tmin, count(prcp_mm) AS dias_estacion_prcp
        FROM dw.intermediate.int_observaciones_diarias WHERE codigo_pais = 'EC'
        GROUP BY anio ORDER BY anio""").df()
    ecuador.to_csv(REP / "cobertura_ecuador.csv", index=False)

    negocio = {
        "anio_analisis": ANIO_ANALISIS,
        "normal_referencia": "promedio 2022-2024 por estación y mes (no es una normal OMS de 30 años)",
        "criterio_paises": f"al menos {MIN_MESES} meses con anomalía y {MIN_ESTACIONES} estaciones por mes",
        "paises_con_anomalia_anual": len(anual),
        "anomalia_mediana_paises_c": round(float(anual.anomalia_c.median()), 2),
        "paises_mas_calidos": anual.head(10).round(2).to_dict("records"),
        "paises_mas_frios": anual.tail(10).round(2).to_dict("records"),
        "anomalia_mensual_mas_alta": mensual.loc[mensual.anomalia_estaciones.idxmax()].astype(str).to_dict(),
        "extremos_tmax_top10": extremos.head(10).astype(str).to_dict("records"),
        "extremos_tmin_top10": extremos.sort_values("tmin_min_c").head(10).astype(str).to_dict("records"),
        "linea_base_ingenua": {
            "paises": len(linea_base),
            "diferencia_abs_mediana_c": round(float(linea_base.diferencia_abs_c.median()), 2),
            "diferencia_abs_max_c": round(float(linea_base.diferencia_abs_c.max()), 2),
            "peor_pais": linea_base.sort_values("diferencia_abs_c").iloc[-1].astype(str).to_dict(),
        },
        "cobertura_ecuador": ecuador.to_dict("records"),
    }
    (REP / "consultas_negocio.json").write_text(json.dumps(negocio, indent=2, ensure_ascii=False), encoding="utf-8")

    figuras(anual, mensual, extremos, linea_base, por_anio)
    print(json.dumps(metricas["totales"], indent=2), json.dumps(negocio["linea_base_ingenua"], indent=2))


def figuras(anual, mensual, extremos, linea_base, por_anio) -> None:
    # 01: anomalía anual por país (10 más cálidos y 10 más fríos), divergente rojo/azul
    sel = anual.head(10)._append(anual.tail(10)).drop_duplicates("codigo_pais").sort_values("anomalia_c")
    fig, ax = plt.subplots(figsize=(8, 6.5))
    colores = [ROJO if v > 0 else AZUL for v in sel.anomalia_c]
    ax.barh(sel.pais.str.slice(0, 28), sel.anomalia_c, color=colores, height=0.7)
    ax.axvline(0, color=TINTA_2, linewidth=0.8)
    for y, v in enumerate(sel.anomalia_c):
        ax.text(v + (0.03 if v >= 0 else -0.03), y, f"{v:+.2f}", va="center", ha="left" if v >= 0 else "right",
                fontsize=8, color=TINTA_2)
    ax.set_xlabel("Anomalía de temperatura media (°C) frente a la normal 2022-2024")
    ax.set_title(f"Anomalía {ANIO_ANALISIS} por país: 10 más cálidos y 10 más fríos")
    ax.grid(axis="y", visible=False)
    ax.margins(x=0.15)
    guardar(fig, "01_anomalia_pais.png")

    # 02: serie mensual global (ponderada por estación frente a ponderada por país)
    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.plot(mensual.mes_inicio, mensual.anomalia_estaciones, color=AZUL, linewidth=2, label="Promedio de estaciones")
    ax.plot(mensual.mes_inicio, mensual.anomalia_paises, color=NARANJA, linewidth=2, label="Promedio de países")
    ax.axhline(0, color=TINTA_2, linewidth=0.8)
    ax.axvspan(mensual.mes_inicio.min(), "2024-12-31", color=REJILLA, alpha=0.5, linewidth=0)
    ax.text(mensual.mes_inicio.min(), ax.get_ylim()[1] * 0.92, " periodo de la normal", color=TINTA_2, fontsize=8)
    ax.set_ylabel("Anomalía (°C)")
    ax.set_title("Anomalía mensual de temperatura media en la red GHCN")
    ax.legend(frameon=False, loc="lower right")
    guardar(fig, "02_anomalia_mensual_global.png")

    # 03: extremos de TMAX por país en el año de análisis
    top = extremos.head(12).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    ax.scatter(top.tmax_max_c, range(len(top)), color=ROJO, s=60, zorder=3)
    for y, (v, est, f) in enumerate(zip(top.tmax_max_c, top.estacion_tmax_max, top.fecha_tmax_max, strict=True)):
        ax.text(v + 0.25, y, f"{v:.1f} °C · {str(est).title()[:24]} · {str(f)[:10]}", va="center", fontsize=8,
                color=TINTA_2)
    ax.set_yticks(range(len(top)), top.pais.str.slice(0, 24))
    ax.set_xlim(top.tmax_max_c.min() - 0.5, top.tmax_max_c.max() + 3.2)
    ax.set_xlabel("Máxima diaria más alta del año (°C, valores con QFLAG vacía)")
    ax.set_title(f"Temperaturas máximas extremas de {ANIO_ANALISIS} por país")
    ax.grid(axis="y", visible=False)
    guardar(fig, "03_extremos_tmax.png")

    # 04: línea base ingenua frente a anomalía por estación
    fig, ax = plt.subplots(figsize=(6.2, 6))
    ax.scatter(linea_base.anomalia_estaciones_c, linea_base.anomalia_ingenua_c, color=AZUL, s=40, alpha=0.85,
               edgecolor=FONDO, linewidth=1.5, zorder=3)
    lim = [min(linea_base[["anomalia_estaciones_c", "anomalia_ingenua_c"]].min()) - 0.3,
           max(linea_base[["anomalia_estaciones_c", "anomalia_ingenua_c"]].max()) + 0.3]
    ax.plot(lim, lim, color=TINTA_2, linewidth=0.8, linestyle="--")
    for _, r in linea_base.nlargest(5, "diferencia_abs_c").iterrows():
        ax.annotate(str(r.pais)[:18], (r.anomalia_estaciones_c, r.anomalia_ingenua_c), fontsize=8, color=TINTA_2,
                    xytext=(5, 3), textcoords="offset points")
    ax.set_xlabel("Anomalía por estación (°C) — la del mart")
    ax.set_ylabel("Anomalía ingenua (°C) — media de la red de cada año")
    ax.set_title(f"Línea base ingenua frente al mart ({ANIO_ANALISIS})")
    guardar(fig, "04_linea_base_ingenua.png")

    # 05: tamaño por formato y año
    import numpy as np

    x = np.arange(len(por_anio))
    fig, ax = plt.subplots(figsize=(8, 4.2))
    for i, (clave, color, etiqueta) in enumerate([("MB_csv", NARANJA, "CSV descomprimido"),
                                                  ("MB_csv_gz", AQUA, "CSV .gz (NOAA)"),
                                                  ("MB_parquet", AZUL, "Parquet zstd (MinIO)")]):
        valores = [f[clave] for f in por_anio]
        ax.bar(x + (i - 1) * 0.27, valores, width=0.25, color=color, label=etiqueta)
    ax.set_xticks(x, [f["anio"] for f in por_anio])
    ax.set_ylabel("MB")
    ax.set_title("Tamaño por año: crudo frente a Parquet")
    ax.legend(frameon=False)
    ax.grid(axis="x", visible=False)
    guardar(fig, "05_crudo_vs_parquet.png")

    # 06: tiempo por año procesado (apilado)
    fig, ax = plt.subplots(figsize=(8, 4.2))
    abajo = np.zeros(len(por_anio))
    for clave, color, etiqueta in [("segundos_descarga", AQUA, "Descarga"),
                                   ("segundos_conversion", AZUL, "CSV → Parquet en MinIO + asset checks")]:
        valores = np.array([f[clave] + (f["segundos_controles"] if clave == "segundos_conversion" else 0)
                            for f in por_anio])
        ax.bar(x, valores, bottom=abajo, width=0.55, color=color, label=etiqueta)
        abajo += valores
    ax.set_xticks(x, [f["anio"] for f in por_anio])
    ax.set_ylabel("Segundos")
    ax.set_title("Tiempo por año procesado (partición de Dagster)")
    ax.legend(frameon=False)
    ax.grid(axis="x", visible=False)
    guardar(fig, "06_tiempo_por_anio.png")


if __name__ == "__main__":
    main()
