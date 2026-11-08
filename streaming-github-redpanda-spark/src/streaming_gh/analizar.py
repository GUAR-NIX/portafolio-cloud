"""Resume las métricas medidas (reports/metricas/*) en reports/resultados.json y dibuja reports/figures/*.png.

Todas las cifras del README salen de aquí: nada se escribe a mano.

    python -m streaming_gh.analizar                 # métricas de todas las corridas + figuras de oro de "principal"
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from streaming_gh.config import DIR_METRICAS, RAIZ

DIR_FIGURAS = RAIZ / "reports" / "figures"
# Paleta categórica validada (orden fijo) + tintas de texto y superficie
AZUL, NARANJA, AQUA, VIOLETA = "#2a78d6", "#eb6834", "#1baf7a", "#6250d6"
TINTA, TINTA_2, REJILLA, FONDO = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
plt.rcParams.update({
    "figure.facecolor": FONDO, "axes.facecolor": FONDO, "savefig.facecolor": FONDO,
    "axes.edgecolor": REJILLA, "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA_2,
    "text.color": TINTA, "axes.grid": True, "grid.color": REJILLA, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.titleweight": "bold", "axes.titlesize": 12,
    "axes.titlelocation": "left", "font.size": 10, "legend.frameon": False, "lines.linewidth": 2,
    "axes.axisbelow": True,
})


def leer_jsonl(ruta: Path) -> pd.DataFrame:
    if not ruta.exists():
        return pd.DataFrame()
    filas = []
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        with contextlib.suppress(json.JSONDecodeError):
            filas.append(json.loads(linea))
    return pd.DataFrame(filas)


def percentil_ponderado(valores: pd.Series, pesos: pd.Series, q: float) -> float:
    orden = np.argsort(valores.to_numpy())
    v, w = valores.to_numpy()[orden], pesos.to_numpy()[orden]
    acumulado = np.cumsum(w) / w.sum()
    return float(v[np.searchsorted(acumulado, q)])


def lotes_unicos(lotes: pd.DataFrame) -> pd.DataFrame:
    """Un lote reprocesado tras una caída aparece dos veces en lotes.jsonl: se queda la última ejecución."""
    return lotes.drop_duplicates(["capa", "lote"], keep="last")


def resumen_corrida(directorio: Path) -> dict:
    r: dict = {"corrida": directorio.name}
    for archivo in ("productor.json", "verificacion.json", "orquestacion.json"):
        if (directorio / archivo).exists():
            r[archivo.removesuffix(".json")] = json.loads((directorio / archivo).read_text())
    lotes = leer_jsonl(directorio / "lotes.jsonl")
    if lotes.empty:
        return r
    r["lotes_repetidos"] = int(lotes.duplicated(["capa", "lote"]).sum())
    lotes = lotes_unicos(lotes)
    capas = {}
    for capa, g in lotes.groupby("capa"):
        con_filas = g[g["filas"] > 0]
        if con_filas.empty:
            continue
        info = {"lotes": int(len(con_filas)), "filas": int(con_filas["filas"].sum()),
                "duracion_lote_mediana_s": round(float(con_filas["duracion_s"].median()), 2)}
        if "lat_p50_s" in con_filas:
            w = con_filas["filas"]
            info["latencia_p50_s"] = round(percentil_ponderado(con_filas["lat_p50_s"], w, 0.5), 2)
            info["latencia_p95_s"] = round(percentil_ponderado(con_filas["lat_p95_s"], w, 0.95), 2)
            info["latencia_max_s"] = round(float(con_filas["lat_max_s"].max()), 2)
        if capa in ("bronce", "plata"):
            ventana = con_filas["t_escrito"].max() - con_filas["t_inicio"].min()
            info["segundos_activos"] = round(float(ventana), 1)
            info["eventos_por_s"] = round(float(con_filas["filas"].sum() / ventana), 0)
            info["eventos_por_s_lote_max"] = round(float((con_filas["filas"] / con_filas["duracion_s"]).max()), 0)
        capas[capa] = info
    r["capas"] = capas
    return r


def recuperacion(directorio: Path) -> dict:
    orq = json.loads((directorio / "orquestacion.json").read_text())
    lotes = leer_jsonl(directorio / "lotes.jsonl")
    eventos = leer_jsonl(directorio / "eventos.jsonl")
    arranques = pd.to_datetime(eventos.loc[eventos["evento"] == "arranque", "ts"]).map(lambda t: t.timestamp())
    caidas = []
    for k in orq["kills"]:
        t_kill = k["ts_kill"]
        rearranque = float(arranques[arranques > t_kill].min())
        bronce_despues = lotes[(lotes["capa"] == "bronce") & (lotes["t_inicio"] > t_kill) & (lotes["filas"] > 0)]
        primer = bronce_despues.sort_values("t_escrito").iloc[0]
        repetidos = lotes[lotes["t_inicio"] > t_kill].merge(
            lotes[lotes["t_inicio"] < t_kill][["capa", "lote"]], on=["capa", "lote"])
        caidas.append({
            "t_productor_s": k["t_productor_s"],
            "segundos_sin_procesar": round(primer["t_escrito"] - t_kill, 1),
            "segundos_hasta_nuevo_driver": round(rearranque - t_kill, 1),
            "primer_lote_bronce_tras_caida": {"lote": int(primer["lote"]), "filas": int(primer["filas"]),
                                               "latencia_max_s": primer.get("lat_max_s")},
            "lotes_reejecutados_tras_caida": repetidos[["capa", "lote"]].to_dict("records"),
        })
    return {"caidas": caidas, "escrituras_omitidas_por_txn": escrituras_omitidas(lotes)}


def escrituras_omitidas(lotes: pd.DataFrame) -> list[dict]:
    """Lotes re-ejecutados tras una caída cuya escritura Delta ignoró (txnAppId + txnVersion ya registrados)."""
    if "escritura_omitida_por_txn" not in lotes:
        return []
    marcados = lotes["escritura_omitida_por_txn"].fillna(False).astype(bool)
    return lotes.loc[marcados, ["capa", "lote", "filas"]].to_dict("records")


# ------------------------------------------------------------------------------------------------------- figuras

def _guardar(fig, nombre: str) -> None:
    DIR_FIGURAS.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(DIR_FIGURAS / nombre, dpi=150)
    plt.close(fig)
    print("  figura:", nombre)


def figura_rendimiento(bench: list[dict]) -> None:
    if not bench:
        return
    etiquetas = [b["etiqueta"] for b in bench]
    x = np.arange(len(bench))
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4))
    ofrecido = [b["productor"]["tasa_originales_eps"] for b in bench]
    procesado = [b["capas"]["bronce"]["eventos_por_s"] for b in bench]
    a1.bar(x - 0.2, ofrecido, 0.38, color=AZUL, label="publicado por el productor")
    a1.bar(x + 0.2, procesado, 0.38, color=NARANJA, label="escrito en bronce (Spark)")
    for i, (o, p) in enumerate(zip(ofrecido, procesado, strict=True)):
        a1.text(i - 0.2, o, f"{o:,.0f}".replace(",", "."), ha="center", va="bottom", fontsize=8, color=TINTA_2)
        a1.text(i + 0.2, p, f"{p:,.0f}".replace(",", "."), ha="center", va="bottom", fontsize=8, color=TINTA_2)
    a1.set_xticks(x, etiquetas)
    a1.set_ylabel("eventos por segundo")
    a1.set_title("Rendimiento por velocidad de reproducción")
    a1.legend(loc="upper left")
    for capa, color, desp in (("bronce", AZUL, -0.2), ("plata", AQUA, 0.2)):
        p50 = [b["capas"][capa]["latencia_p50_s"] for b in bench]
        p95 = [b["capas"][capa]["latencia_p95_s"] for b in bench]
        a2.bar(x + desp, p50, 0.38, color=color, label=f"{capa} p50")
        a2.scatter(x + desp, p95, color=TINTA, marker="_", s=300, zorder=3)
    a2.scatter([], [], color=TINTA, marker="_", s=200, label="p95")
    a2.set_xticks(x, etiquetas)
    a2.set_ylabel("segundos (publicación → escrito en Delta)")
    a2.set_title("Latencia de extremo a extremo")
    a2.legend(loc="upper left")
    _guardar(fig, "rendimiento_velocidades.png")


def figura_latencia_principal(directorio: Path) -> None:
    lotes = lotes_unicos(leer_jsonl(directorio / "lotes.jsonl"))
    if lotes.empty:
        return
    t0 = lotes["t_inicio"].min()
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 6), sharex=True)
    b = lotes[(lotes["capa"] == "bronce") & (lotes["filas"] > 0)].sort_values("t_escrito")
    a1.plot((b["t_escrito"] - t0) / 60, b["filas"] / b["duracion_s"].clip(lower=0.5) / 1000, color=AZUL, lw=1.2)
    a1.set_ylabel("miles de eventos/s\npor lote de bronce")
    a1.set_title("Corrida principal: ritmo y latencia de cada micro-lote")
    for capa, color in (("bronce", AZUL), ("plata", AQUA)):
        g = lotes[(lotes["capa"] == capa) & (lotes["filas"] > 0)].sort_values("t_escrito")
        a2.plot((g["t_escrito"] - t0) / 60, g["lat_p50_s"], color=color, lw=1.2, label=f"{capa} p50")
    a2.set_ylabel("latencia (s)")
    a2.set_xlabel("minutos desde el arranque")
    a2.legend(loc="upper right")
    _guardar(fig, "latencia_principal.png")


def figura_recuperacion(directorio: Path) -> None:
    orq = json.loads((directorio / "orquestacion.json").read_text())
    lotes = lotes_unicos(leer_jsonl(directorio / "lotes.jsonl"))
    t0 = orq["ts_productor_inicio"]
    prod = json.loads((directorio / "productor.json").read_text())
    fig, ax = plt.subplots(figsize=(11, 4.2))
    # publicado: a ritmo constante entre inicio y fin del productor (el reloj virtual es lineal)
    ax.plot([0, prod["duracion_originales_s"]], [0, prod["enviados"] / 1e6], color=TINTA_2, lw=1.5, ls="--",
            label="publicado en Redpanda")
    for capa, color in (("bronce", AZUL), ("plata", AQUA)):
        g = lotes[lotes["capa"] == capa].sort_values("t_escrito")
        ax.step(g["t_escrito"] - t0, g["filas"].cumsum() / 1e6, where="post", color=color, label=capa)
    for i, k in enumerate(orq["kills"]):
        ax.axvline(k["ts_kill"] - t0, color=NARANJA, lw=1.5)
        ax.text(k["ts_kill"] - t0 + 2, 0.05 + 0.1 * i, "docker kill", color=NARANJA, fontsize=9)
    ax.set_xlabel("segundos desde el inicio del productor")
    ax.set_ylabel("millones de eventos (acumulado)")
    ax.set_title(f"Recuperación: Spark muere {len(orq['kills'])} veces y alcanza lo publicado sin duplicar ni perder")
    ax.legend(loc="upper left")
    _guardar(fig, "recuperacion.png")


def figuras_oro(corrida: str) -> dict:
    from streaming_gh.consultas_oro import conectar

    con = conectar(corrida)
    salida = {}
    hora = con.execute("""SELECT HOUR(ventana_inicio) AS hora,
                                 SUM(eventos) / COUNT(DISTINCT CAST(ventana_inicio AS DATE)) AS eventos
                          FROM actividad_5min GROUP BY 1 ORDER BY 1""").df()
    fig, ax = plt.subplots(figsize=(10, 3.8))
    ax.bar(hora["hora"], hora["eventos"] / 1000, color=AZUL, width=0.8)
    ax.set_xticks(range(0, 24, 2))
    ax.set_xlabel("hora del día (UTC)")
    ax.set_ylabel("miles de eventos por hora")
    ax.set_title("Actividad pública de GitHub por hora del día (promedio 16–18 sep 2025)")
    _guardar(fig, "actividad_por_hora.png")
    salida["hora_pico_utc"] = int(hora.loc[hora["eventos"].idxmax(), "hora"])
    salida["hora_valle_utc"] = int(hora.loc[hora["eventos"].idxmin(), "hora"])

    serie = con.execute("""SELECT ventana_inicio, tipo, eventos FROM actividad_5min ORDER BY 1""").df()
    picos = (con.execute("SELECT ventana_inicio, tipo, eventos, z FROM picos_actividad").df()
             if _existe(con, "picos_actividad") else pd.DataFrame(columns=["ventana_inicio", "tipo", "eventos", "z"]))
    total = serie.groupby("ventana_inicio", as_index=False)["eventos"].sum()
    tipos = ["IssueCommentEvent", "ReleaseEvent"]
    fig, ejes = plt.subplots(3, 1, figsize=(11, 7.5), sharex=True)
    ejes[0].plot(total["ventana_inicio"], total["eventos"] / 1000, color=AZUL, lw=1.1)
    hueco = total.loc[total["eventos"].idxmin()]
    ejes[0].annotate("hueco en la fuente (GH Archive)", (hueco["ventana_inicio"], hueco["eventos"] / 1000),
                     xytext=(-190, 10), textcoords="offset points", color=TINTA_2, fontsize=9,
                     arrowprops={"arrowstyle": "-", "color": TINTA_2, "lw": 0.8})
    ejes[0].set_ylabel("miles / 5 min")
    ejes[0].set_title("Todos los eventos cada 5 minutos")
    for ax, tipo in zip(ejes[1:], tipos, strict=True):
        s_t = serie[serie["tipo"] == tipo]
        p_t = picos[picos["tipo"] == tipo]
        ax.plot(s_t["ventana_inicio"], s_t["eventos"], color=AZUL, lw=1.1)
        ax.scatter(p_t["ventana_inicio"], p_t["eventos"], color=NARANJA, s=36, zorder=3, edgecolor=FONDO,
                   linewidth=1.5, label=f"pico detectado en streaming ({len(p_t)})")
        ax.set_ylabel("eventos / 5 min")
        ax.set_title(f"{tipo}: picos con z ≥ 3 frente a la hora anterior")
        ax.legend(loc="upper left")
    salida["picos_por_tipo"] = picos.groupby("tipo").size().sort_values(ascending=False).to_dict()
    salida["hueco_fuente"] = {"desde": str(hueco["ventana_inicio"]), "eventos_min_5min": int(hueco["eventos"])}
    _guardar(fig, "actividad_5min_picos.png")

    repos = con.execute((RAIZ / "sql" / "01_repos_tendencia.sql").read_text(encoding="utf-8")).df()
    fig, ax = plt.subplots(figsize=(10, 5.2))
    r = repos.iloc[::-1]
    ax.barh(r["repo_nombre"], r["estrellas"], color=AZUL, height=0.7)
    for y, (e, f) in enumerate(zip(r["estrellas"], r["forks"], strict=True)):
        ax.text(e, y, f"  {e:,}  ({f:,} forks)".replace(",", "."), va="center", fontsize=8, color=TINTA_2)
    ax.set_xlabel("estrellas nuevas (WatchEvent) en 3 días")
    ax.set_title("Repos en tendencia")
    ax.grid(axis="y", visible=False)
    _guardar(fig, "repos_tendencia.png")
    salida["repo_top"] = {"repo": repos.iloc[0]["repo_nombre"], "estrellas": int(repos.iloc[0]["estrellas"])}

    leng = con.execute((RAIZ / "sql" / "03_lenguajes.sql").read_text(encoding="utf-8")).df()
    fig, ax = plt.subplots(figsize=(10, 4.8))
    lg = leng.iloc[::-1]
    ax.barh(lg["lenguaje"], lg["eventos_pr"] / 1000, color=AZUL, height=0.7)
    for y, p in enumerate(lg["pct_eventos_pr"]):
        ax.text(lg["eventos_pr"].iloc[y] / 1000, y, f"  {p:.1f} %", va="center", fontsize=8, color=TINTA_2)
    ax.set_xlabel("miles de eventos de pull request")
    ax.set_title("Lenguajes por actividad de pull requests")
    ax.grid(axis="y", visible=False)
    _guardar(fig, "lenguajes.png")
    salida["lenguaje_top"] = leng.iloc[0]["lenguaje"]
    return salida


def _existe(con, vista: str) -> bool:
    return bool(con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{vista}'")
                .fetchone()[0])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corrida-oro", default="principal")
    parser.add_argument("--sin-oro", action="store_true")
    args = parser.parse_args()
    resultados = {"corridas": {}}
    for d in sorted(p for p in DIR_METRICAS.iterdir() if p.is_dir()):
        resultados["corridas"][d.name] = resumen_corrida(d)
    if (DIR_METRICAS / "falla_inyectada" / "lotes.jsonl").exists():
        lotes_falla = leer_jsonl(DIR_METRICAS / "falla_inyectada" / "lotes.jsonl")
        eventos_falla = leer_jsonl(DIR_METRICAS / "falla_inyectada" / "eventos.jsonl")
        resultados["falla_inyectada"] = {
            "falla": eventos_falla[eventos_falla["evento"] == "falla_inyectada"].drop(columns=["args"], errors="ignore")
            .dropna(axis=1).to_dict("records"),
            "escrituras_omitidas_por_txn": escrituras_omitidas(lotes_falla)}
    if (DIR_METRICAS / "recuperacion" / "orquestacion.json").exists():
        resultados["recuperacion"] = recuperacion(DIR_METRICAS / "recuperacion")
        figura_recuperacion(DIR_METRICAS / "recuperacion")
    bench = []
    for nombre, etiqueta in (("bench_60x", "60x"), ("bench_600x", "600x"), ("bench_max", "máxima")):
        c = resultados["corridas"].get(nombre)
        if c and "capas" in c and "productor" in c:
            bench.append(c | {"etiqueta": etiqueta})
    figura_rendimiento(bench)
    if (DIR_METRICAS / args.corrida_oro).exists():
        figura_latencia_principal(DIR_METRICAS / args.corrida_oro)
        if not args.sin_oro:
            resultados["oro"] = figuras_oro(args.corrida_oro)
    destino = RAIZ / "reports" / "resultados.json"
    destino.write_text(json.dumps(resultados, indent=2, ensure_ascii=False, default=str))
    print(f"Resultados en {destino}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
