"""Cifras reales del pipeline -> reports/resultados.json + reports/figures/*.png

Lee el almacén (`datos`) y los metadatos de Airflow (`airflow`) en el Postgres de docker compose
(localhost:5433). Credenciales: las de solo-local de docker-compose.yml (se pueden cambiar por variables).

    .venv\\Scripts\\python scripts\\exportar_resultados.py
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import matplotlib
import pandas as pd
import psycopg2

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
FIG = RAIZ / "reports" / "figures"
HOST, PUERTO = os.environ.get("PG_HOST", "localhost"), int(os.environ.get("PG_PUERTO", "5433"))

# Paleta de referencia (validada para daltonismo en el orden de abajo); texto siempre en tinta neutra
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
TINTA, TINTA_2, GRILLA = "#0b0b0b", "#52514e", "#e4e3df"
REGIONES = ["Costa", "Sierra", "Amazonía", "Insular"]
MESES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]


def mes_corto(fecha) -> str:
    fecha = pd.Timestamp(fecha).replace(tzinfo=None)
    return f"{MESES[fecha.month - 1]}\n{fecha:%Y}"


def consulta(base: str, sql: str, usuario: str, clave: str) -> pd.DataFrame:
    with psycopg2.connect(host=HOST, port=PUERTO, dbname=base, user=usuario, password=clave) as c:
        return pd.read_sql_query(sql, c)


def datos(sql):
    return consulta("datos", sql, os.environ.get("PG_USUARIO_DATOS", "datos"),
                    os.environ.get("PG_CLAVE_DATOS", "datos"))


def airflow(sql):
    return consulta("airflow", sql, "airflow", os.environ.get("PG_CLAVE_AIRFLOW", "airflow"))


def estilo(ax, titulo, subtitulo=None):
    ax.set_title(titulo, loc="left", fontsize=12, fontweight="bold", color=TINTA, pad=18 if subtitulo else 8)
    if subtitulo:
        ax.text(0, 1.02, subtitulo, transform=ax.transAxes, fontsize=9, color=TINTA_2)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(GRILLA)
    ax.tick_params(colors=TINTA_2, labelsize=9)
    ax.grid(axis="y", color=GRILLA, linewidth=0.8)
    ax.set_axisbelow(True)


# ------------------------------------------------------------------------------------------------ cifras
def resumen_airflow() -> dict:
    corridas = airflow("""
        SELECT dag_id, state, run_type, count(*) AS n,
               min(start_date) AS primera, max(end_date) AS ultima,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM end_date - start_date)) AS mediana_s
        FROM dag_run GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""")
    tareas = airflow("""
        SELECT dag_id, task_id, state, count(*) AS n,
               round(avg(duration)::numeric, 2) AS media_s,
               round((percentile_cont(0.5) WITHIN GROUP (ORDER BY duration))::numeric, 2) AS mediana_s,
               round((percentile_cont(0.95) WITHIN GROUP (ORDER BY duration))::numeric, 2) AS p95_s,
               sum(CASE WHEN try_number > 1 THEN 1 ELSE 0 END) AS con_reintento
        FROM task_instance GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""")
    return {
        "corridas": json.loads(corridas.to_json(orient="records", date_format="iso")),
        "tareas": json.loads(tareas.to_json(orient="records")),
    }


def resumen_datos() -> dict:
    capas = {}
    for tabla in ["raw.clima_respuestas", "raw.banco_mundial_respuestas", "staging.clima_diario",
                  "staging.indicadores_bm", "auditoria.cargas", "auditoria.controles_calidad"]:
        capas[tabla] = int(datos(f"SELECT count(*) AS n FROM {tabla}").n[0])
    marts = datos("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'marts' ORDER BY 1""")
    for t in marts.table_name:
        capas[f"marts.{t}"] = int(datos(f"SELECT count(*) AS n FROM marts.{t}").n[0])
    tamanos = datos("""SELECT n.nspname AS esquema, sum(pg_total_relation_size(c.oid)) AS bytes
                       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                       WHERE n.nspname IN ('raw', 'staging', 'marts', 'auditoria') AND c.relkind = 'r'
                       GROUP BY 1 ORDER BY 1""")
    clima = datos("""SELECT min(fecha) AS desde, max(fecha) AS hasta, count(DISTINCT fecha) AS dias,
                            count(*) FILTER (WHERE fuente = 'pronostico') AS filas_provisionales
                     FROM staging.clima_diario""").iloc[0]
    raw_clima = datos("""SELECT fuente, count(*) AS dias, sum(bytes) AS bytes, sum(descargas) AS descargas
                         FROM raw.clima_respuestas GROUP BY 1""")
    bm = datos("""SELECT min(anio) AS desde, max(anio) AS hasta, count(DISTINCT anio) AS anios,
                         count(DISTINCT pais_iso3) AS paises, count(DISTINCT indicador) AS indicadores
                  FROM staging.indicadores_bm""").iloc[0]
    registros_bm = int(datos("SELECT sum(registros) AS n FROM raw.banco_mundial_respuestas").n[0])
    controles = datos("""SELECT capa, critico, ok, count(*) AS n FROM auditoria.controles_calidad
                         GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""")
    fallos = datos("""SELECT capa, control, count(*) AS veces, min(valor) AS valor_min, max(valor) AS valor_max,
                             min(registrado_en) AS primera
                      FROM auditoria.controles_calidad WHERE NOT ok GROUP BY 1, 2 ORDER BY 1, 2""")
    tiempos = datos("""SELECT dag_id, capa, count(*) AS cargas, sum(filas) AS filas,
                              round(avg(segundos), 3) AS media_s, round(max(segundos), 3) AS max_s
                       FROM auditoria.cargas GROUP BY 1, 2 ORDER BY 1, 2""")
    return {
        "filas_por_tabla": capas,
        "bytes_por_esquema": {r.esquema: int(r.bytes) for r in tamanos.itertuples()},
        "clima": {"desde": str(clima.desde), "hasta": str(clima.hasta), "dias": int(clima.dias),
                  "filas_provisionales": int(clima.filas_provisionales),
                  "raw_por_fuente": json.loads(raw_clima.to_json(orient="records"))},
        "banco_mundial": {"desde": int(bm.desde), "hasta": int(bm.hasta), "anios": int(bm.anios),
                          "paises": int(bm.paises), "indicadores": int(bm.indicadores),
                          "registros_crudos": registros_bm},
        "controles": json.loads(controles.to_json(orient="records")),
        "controles_fallidos": json.loads(fallos.to_json(orient="records", date_format="iso")),
        "tiempos_carga": json.loads(tiempos.to_json(orient="records")),
        "resumen_ciudad": json.loads(datos("SELECT * FROM marts.resumen_ciudad ORDER BY temp_media_c DESC")
                                     .to_json(orient="records", date_format="iso")),
        "ecuador_ultimo_anio": json.loads(datos("""
            SELECT DISTINCT ON (indicador) indicador, nombre_indicador, anio, valor_ecuador, promedio_pares,
                   valor_america_latina, puesto_ecuador, paises_con_dato
            FROM marts.ecuador_vs_region ORDER BY indicador, anio DESC""").to_json(orient="records")),
    }


# ----------------------------------------------------------------------------------------------- figuras
PAUSA_S = 300     # un hueco de más de 5 min entre cargas es una pausa (PC apagado), no tiempo de trabajo


def ritmo_backfill() -> tuple[pd.DataFrame, dict]:
    """Cargas a staging del DAG de clima en orden, con el tiempo ACTIVO acumulado (sin las pausas)."""
    d = datos("""SELECT registrado_en FROM auditoria.cargas
                 WHERE dag_id = 'ingesta_clima_ecuador' AND capa = 'staging' ORDER BY registrado_en""")
    d["registrado_en"] = pd.to_datetime(d.registrado_en, utc=True)
    huecos = d.registrado_en.diff().dt.total_seconds().fillna(0)
    d["activo_min"] = huecos.where(huecos <= PAUSA_S, 0).cumsum() / 60
    d["acumulado"] = range(1, len(d) + 1)
    ultimos = d[d.registrado_en > d.registrado_en.max() - pd.Timedelta(minutes=30)]
    resumen = {"cargas_staging": len(d), "minutos_activos": round(float(d.activo_min.iloc[-1]), 1),
               "pausas": int((huecos > PAUSA_S).sum()),
               "dias_por_minuto_ultimos_30min": round(len(ultimos) / max(
                   float(ultimos.activo_min.iloc[-1] - ultimos.activo_min.iloc[0]), 1e-9), 2)}
    return d, resumen


def figura_backfill(d: pd.DataFrame, resumen: dict):
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.plot(d.activo_min, d.acumulado, color=SERIES[0], linewidth=2)
    ax.set_xlabel("minutos de trabajo (sin las pausas por apagado del equipo)", color=TINTA_2, fontsize=9)
    ax.set_ylabel("días cargados en staging", color=TINTA_2, fontsize=9)
    ax.annotate(f"{resumen['cargas_staging']} cargas · {resumen['minutos_activos']:.0f} min",
                (d.activo_min.iloc[-1], len(d)), xytext=(-8, -4), textcoords="offset points", ha="right",
                va="top", fontsize=9, color=TINTA)
    estilo(ax, "Backfill del DAG de clima: un día por ejecución",
           f"Incluye re-ejecuciones · {resumen['pausas']} pausas omitidas · la pendiente sube al quitar el "
           f"asset con URI (~9 s menos por tarea)")
    fig.tight_layout()
    fig.savefig(FIG / "backfill_clima.png", dpi=150)
    plt.close(fig)


def figura_temperatura_region():
    d = datos("SELECT mes_inicio, region, temp_media_c FROM marts.clima_mensual_region ORDER BY 1")
    d["mes_inicio"] = pd.to_datetime(d.mes_inicio)
    fig, ax = plt.subplots(figsize=(8, 4))
    for color, region in zip(SERIES, REGIONES, strict=True):
        s = d[d.region == region]
        ax.plot(s.mes_inicio, s.temp_media_c.astype(float), color=color, linewidth=2, marker="o", markersize=4,
                label=region)
        ax.annotate(region, (s.mes_inicio.iloc[-1], float(s.temp_media_c.iloc[-1])), xytext=(6, 0),
                    textcoords="offset points", va="center", fontsize=9, color=TINTA)
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: mes_corto(mdates.num2date(x))))
    ax.set_ylabel("°C (promedio de las ciudades)", color=TINTA_2, fontsize=9)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=4, frameon=False, fontsize=9)
    estilo(ax, "Temperatura media mensual por región", "marts.clima_mensual_region · Open-Meteo (ERA5)")
    ax.set_xlim(right=d.mes_inicio.max() + pd.Timedelta(days=45))
    fig.tight_layout()
    fig.savefig(FIG / "temperatura_region.png", dpi=150)
    plt.close(fig)


def figura_lluvia_heatmap():
    d = datos("""SELECT m.mes_inicio, m.ciudad, m.lluvia_total_mm, m.dias_con_dato
                 FROM marts.clima_mensual_ciudad m ORDER BY 1""")
    orden = datos("SELECT ciudad FROM marts.resumen_ciudad ORDER BY lluvia_anualizada_mm DESC").ciudad.tolist()
    tabla = d.pivot(index="ciudad", columns="mes_inicio", values="lluvia_total_mm").astype(float).loc[orden]
    fig, ax = plt.subplots(figsize=(10, 5.2))
    im = ax.imshow(tabla.values, aspect="auto", cmap="Blues")
    ax.set_yticks(range(len(tabla.index)), tabla.index)
    ax.set_xticks(range(len(tabla.columns)), [mes_corto(c) for c in tabla.columns])
    for i in range(tabla.shape[0]):
        for j in range(tabla.shape[1]):
            v = tabla.values[i, j]
            if pd.notna(v):
                ax.text(j, i, f"{v:.0f}", ha="center", va="center", fontsize=7,
                        color="white" if v > tabla.values[~pd.isna(tabla.values)].max() * 0.55 else TINTA)
    barra = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.01)
    barra.set_label("mm en el mes", fontsize=9, color=TINTA_2)
    estilo(ax, "Lluvia mensual por ciudad (mm)",
           "marts.clima_mensual_ciudad · ordenado de la ciudad más lluviosa a la más seca · meses extremos parciales")
    ax.grid(False)
    fig.tight_layout()
    fig.savefig(FIG / "lluvia_mensual_ciudad.png", dpi=150)
    plt.close(fig)


def figura_pib():
    d = datos("""SELECT anio, valor_ecuador, promedio_pares, valor_america_latina FROM marts.ecuador_vs_region
                 WHERE indicador = 'NY.GDP.PCAP.CD' ORDER BY anio""")
    fig, ax = plt.subplots(figsize=(8, 4))
    series = [("valor_ecuador", "Ecuador"), ("promedio_pares", "Promedio de 7 pares"),
              ("valor_america_latina", "América Latina y el Caribe")]
    for color, (col, nombre) in zip(SERIES[:3], series, strict=True):
        ax.plot(d.anio, d[col].astype(float), color=color, linewidth=2, label=nombre)
        ax.annotate(f"{nombre}\n{float(d[col].iloc[-1]):,.0f}", (d.anio.iloc[-1], float(d[col].iloc[-1])),
                    xytext=(6, 0), textcoords="offset points", va="center", fontsize=8, color=TINTA)
    ax.set_xlim(d.anio.min(), d.anio.max() + 6)
    ax.yaxis.set_major_formatter(matplotlib.ticker.StrMethodFormatter("{x:,.0f}"))
    ax.set_ylabel("USD corrientes por habitante", color=TINTA_2, fontsize=9)
    ax.legend(loc="upper left", frameon=False, fontsize=9)
    estilo(ax, "PIB per cápita: Ecuador frente a la región",
           "marts.ecuador_vs_region · pares: COL, PER, BOL, CHL, BRA, ARG, MEX · Banco Mundial")
    fig.tight_layout()
    fig.savefig(FIG / "pib_ecuador_vs_region.png", dpi=150)
    plt.close(fig)


def figura_duracion_tareas(tareas: list[dict]):
    d = pd.DataFrame(tareas)
    # solo las tareas de la versión actual (construir/controles de marts se fusionaron en publicar_marts)
    d = d[(d.state == "success") & ~((d.dag_id == "construir_marts") & (d.task_id != "publicar_marts"))].copy()
    d["nombre"] = d.dag_id.str.replace("ingesta_", "") + "\n" + d.task_id
    d = d.sort_values("mediana_s")
    fig, ax = plt.subplots(figsize=(8, 4.2))
    y = range(len(d))
    ax.barh(y, d.mediana_s.astype(float), color=SERIES[0], height=0.55, label="mediana")
    ax.scatter(d.p95_s.astype(float), y, color=TINTA, s=18, zorder=3, label="p95")
    for i, (_, r) in enumerate(d.iterrows()):
        etiqueta = f"mediana {float(r.mediana_s):.1f} s · p95 {float(r.p95_s):.1f} s · n={int(r.n)}"
        ax.text(float(r.p95_s) + 0.3, i, etiqueta, va="center", fontsize=8, color=TINTA_2)
    ax.set_yticks(list(y), d.nombre, fontsize=8)
    ax.set_xlabel("segundos por instancia de tarea", color=TINTA_2, fontsize=9)
    ax.set_xlim(right=float(d.p95_s.max()) * 1.9)
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    estilo(ax, "Duración de las tareas exitosas (mediana y p95)", "tabla task_instance de Airflow")
    ax.grid(axis="x", color=GRILLA, linewidth=0.8)
    ax.grid(axis="y", visible=False)
    fig.tight_layout()
    fig.savefig(FIG / "duracion_tareas.png", dpi=150)
    plt.close(fig)


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    cargas, ritmo = ritmo_backfill()
    resultado = {"generado": datetime.now().isoformat(timespec="seconds"), "airflow": resumen_airflow(),
                 "datos": resumen_datos(), "backfill_clima": ritmo}
    salida = RAIZ / "reports" / "resultados.json"
    salida.write_text(json.dumps(resultado, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    figura_backfill(cargas, ritmo)
    figura_temperatura_region()
    figura_lluvia_heatmap()
    figura_pib()
    figura_duracion_tareas(resultado["airflow"]["tareas"])
    print(f"Escrito {salida.relative_to(RAIZ)} y {len(list(FIG.glob('*.png')))} figuras en reports/figures/")


if __name__ == "__main__":
    main()
