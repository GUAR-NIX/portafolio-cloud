"""Tablero de la capa oro (Streamlit + DuckDB sobre Delta en MinIO). Se refresca solo mientras el pipeline corre.

    docker compose up -d dashboard      →  http://localhost:8501
"""

from __future__ import annotations

import json
import os

import pandas as pd
import streamlit as st

from streaming_gh.config import DIR_METRICAS
from streaming_gh.consultas_oro import conectar

CORRIDA = os.environ.get("CORRIDA", "principal")
st.set_page_config(page_title="GitHub en tiempo real", page_icon="📈", layout="wide")


@st.cache_resource
def conexion():
    return conectar(CORRIDA, memoria="400MB")  # el contenedor tiene 768 MB


@st.cache_data(ttl=15)
def q(sql: str) -> pd.DataFrame:
    try:
        return conexion().cursor().execute(sql).df()  # un cursor por consulta: la conexión no es segura entre hilos
    except Exception:  # noqa: BLE001 - tabla aún vacía o inexistente al arrancar
        return pd.DataFrame()


st.title("Actividad pública de GitHub en tiempo real")
st.caption(f"Corrida `{CORRIDA}` · capa oro en Delta Lake (MinIO) · eventos de GH Archive reproducidos por Redpanda "
           "y procesados con Spark Structured Streaming")

resumen = q("""SELECT SUM(eventos) AS eventos, COUNT(DISTINCT ventana_inicio) AS ventanas,
                      MIN(ventana_inicio) AS desde, MAX(ventana_fin) AS hasta FROM actividad_5min""")
repos = q("""SELECT SUM(estrellas) AS estrellas, SUM(forks) AS forks, APPROX_COUNT_DISTINCT(repo_id) AS repos
              FROM repos_hora""")
if resumen.empty or pd.isna(resumen.iloc[0]["eventos"]):
    st.info("Todavía no hay ventanas cerradas en oro. Arranca el pipeline y el productor (ver la guía).")
    st.stop()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Eventos en ventanas cerradas", f"{int(resumen.iloc[0]['eventos']):,}".replace(",", "."))
c2.metric("Repos activos (aprox.)", f"{int(repos.iloc[0]['repos'] or 0):,}".replace(",", "."))
c3.metric("Estrellas nuevas", f"{int(repos.iloc[0]['estrellas'] or 0):,}".replace(",", "."))
c4.metric("Hasta (tiempo de evento, UTC)", str(resumen.iloc[0]["hasta"])[:16])

st.subheader("Eventos cada 5 minutos por tipo")
serie = q("""SELECT ventana_inicio, tipo, eventos FROM actividad_5min
             WHERE tipo IN ('PushEvent','CreateEvent','PullRequestEvent','IssueCommentEvent','WatchEvent')""")
if not serie.empty:
    st.line_chart(serie.pivot_table(index="ventana_inicio", columns="tipo", values="eventos"), height=300)

izq, der = st.columns(2)
with izq:
    st.subheader("Repos en tendencia (estrellas)")
    st.dataframe(q("""SELECT repo_nombre AS repo, SUM(estrellas) AS estrellas, SUM(forks) AS forks,
                             MAX(lenguaje) AS lenguaje FROM repos_hora
                      WHERE estrellas > 0 GROUP BY 1 ORDER BY 2 DESC LIMIT 15"""),
                 hide_index=True, use_container_width=True)
with der:
    st.subheader("Lenguajes por actividad de pull requests")
    leng = q("""SELECT lenguaje, SUM(eventos_pr) AS eventos_pr FROM lenguajes_hora
                GROUP BY 1 ORDER BY 2 DESC LIMIT 12""")
    if not leng.empty:
        st.bar_chart(leng.set_index("lenguaje"), horizontal=True, height=380)

izq, der = st.columns(2)
with izq:
    st.subheader("Actividad por hora del día (UTC)")
    hora = q("""SELECT HOUR(ventana_inicio) AS hora, SUM(eventos) / COUNT(DISTINCT CAST(ventana_inicio AS DATE))
                AS eventos FROM actividad_5min GROUP BY 1 ORDER BY 1""")
    if not hora.empty:
        st.bar_chart(hora.set_index("hora"), height=300)
with der:
    st.subheader("Picos de estrellas detectados")
    st.dataframe(q("""SELECT ventana_inicio AS hora, repo_nombre AS repo, estrellas, linea_base,
                             veces_sobre_base FROM picos_repos ORDER BY estrellas DESC LIMIT 15"""),
                 hide_index=True, use_container_width=True)

st.subheader("Salud del pipeline")
ruta = DIR_METRICAS / CORRIDA / "lotes.jsonl"
if ruta.exists():
    lotes = pd.DataFrame([json.loads(x) for x in ruta.read_text(encoding="utf-8").splitlines() if x.strip()])
    lotes = lotes[lotes["filas"] > 0]
    tabla = (lotes.groupby("capa")
             .agg(lotes=("lote", "count"), filas=("filas", "sum"), lat_p50_s=("lat_p50_s", "median"),
                  lat_p95_s=("lat_p95_s", "median"), duracion_s=("duracion_s", "median"))
             .round(2).reset_index())
    st.dataframe(tabla, hide_index=True, use_container_width=True)
    st.caption("Latencia = momento en que el micro-lote quedó escrito en Delta − hora en que el productor publicó el "
               "mensaje. En oro es la frescura de la ventana e incluye la espera del watermark (10 min de tiempo de "
               "evento ÷ factor de aceleración).")
