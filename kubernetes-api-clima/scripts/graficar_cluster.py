"""Figuras del README a partir de reports/carga_cluster*.csv y reports/rolling_update.json."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RAIZ = Path(__file__).resolve().parents[1]
REPORTES = RAIZ / "reports"
FIGURAS = REPORTES / "figures"
AZUL, NARANJA, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
ETIQUETAS = {"fijo-1": "1 réplica fija", "fijo-4": "4 réplicas fijas",
             "hpa": "HPA vía ClusterIP", "hpa-ingress": "HPA vía Ingress"}
TINTA, TINTA_2, FONDO, REJILLA = "#0b0b0b", "#52514e", "#fcfcfb", "#e4e3df"


def estilo() -> None:
    plt.rcParams.update({
        "figure.facecolor": FONDO, "axes.facecolor": FONDO, "axes.edgecolor": REJILLA,
        "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA_2,
        "axes.titlecolor": TINTA, "axes.titlesize": 11, "axes.titleweight": "bold",
        "axes.grid": True, "grid.color": REJILLA, "grid.linewidth": 0.8,
        "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False,
        "font.size": 9.5,
    })


def historia(nombre: str) -> pd.DataFrame:
    datos = pd.read_csv(REPORTES / f"carga_cluster_{nombre}_stats_history.csv")
    datos = datos[(datos["Name"] == "Aggregated") & (datos["User Count"] > 0)].copy()
    datos["segundo"] = datos["Timestamp"] - datos["Timestamp"].min()
    return datos


def etiquetar_barras(eje, barras, formato: str) -> None:
    for barra in barras:
        eje.annotate(formato.format(barra.get_height()),
                     (barra.get_x() + barra.get_width() / 2, barra.get_height()),
                     ha="center", va="bottom", xytext=(0, 3), textcoords="offset points",
                     color=TINTA, fontsize=9)


def comparacion(resumen: dict) -> None:
    escenarios = resumen["escenarios"]
    etiquetas = [ETIQUETAS[e["escenario"]] for e in escenarios]
    posiciones = range(len(escenarios))
    fig, (eje1, eje2) = plt.subplots(1, 2, figsize=(12, 4.2))
    barras = eje1.bar(posiciones, [e["solicitudes_por_segundo"] for e in escenarios],
                      color=AZUL, width=0.55, edgecolor=FONDO, linewidth=2)
    etiquetar_barras(eje1, barras, "{:.0f}")
    eje1.set(title="Rendimiento (solicitudes/s)", xticks=list(posiciones), xticklabels=etiquetas)
    ancho = 0.36
    p50 = eje2.bar([p - ancho / 2 for p in posiciones], [e["latencia_p50_ms"] for e in escenarios],
                   width=ancho, color=AZUL, label="p50", edgecolor=FONDO, linewidth=2)
    p95 = eje2.bar([p + ancho / 2 for p in posiciones], [e["latencia_p95_ms"] for e in escenarios],
                   width=ancho, color=NARANJA, label="p95", edgecolor=FONDO, linewidth=2)
    etiquetar_barras(eje2, p50, "{:.0f}")
    etiquetar_barras(eje2, p95, "{:.0f}")
    eje2.set(title="Latencia (ms)", xticks=list(posiciones), xticklabels=etiquetas)
    eje2.legend(loc="upper right")
    usuarios = escenarios[0]["usuarios"]
    errores = sum(e["errores"] for e in escenarios)
    fig.suptitle(f"Job de Locust dentro de kind · {usuarios} usuarios · errores totales: {errores}",
                 color=TINTA_2, fontsize=9.5)
    fig.tight_layout()
    fig.savefig(FIGURAS / "03_carga_cluster_comparacion.png", dpi=160)
    plt.close(fig)


def escalado(resumen: dict) -> None:
    muestras = pd.read_csv(REPORTES / "carga_cluster_hpa-ingress_muestras.csv")
    hpa = next(e for e in resumen["escenarios"] if e["escenario"] == "hpa-ingress")
    fin_carga = hpa["fin_carga_unix_nodo"] - hpa["inicio_carga_unix_nodo"]
    objetivo = resumen["hpa"]["objetivo_cpu_pct"]
    fig, ejes = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
    for nombre, color in (("hpa-ingress", AZUL), ("hpa", NARANJA)):
        carga = historia(nombre)
        ejes[0].plot(carga["segundo"], carga["Requests/s"], color=color, linewidth=2,
                     label=ETIQUETAS[nombre])
    ejes[0].set(title="Solicitudes por segundo con HPA (Locust)", ylabel="sol./s")
    ejes[0].legend(loc="center right")
    ejes[1].step(muestras["segundo"], muestras["listas"], where="post", color=AZUL,
                 linewidth=2, label="réplicas listas")
    ejes[1].step(muestras["segundo"], muestras["deseadas"], where="post", color=NARANJA,
                 linewidth=2, linestyle="--", label="deseadas por el HPA")
    ejes[1].set(title="Réplicas de la API (corrida vía Ingress)", ylabel="pods",
                yticks=range(0, int(muestras["listas"].max()) + 2))
    ejes[1].legend(loc="upper right")
    ejes[2].plot(muestras["segundo"], muestras["cpu_pct_del_request"], color=AQUA, linewidth=2)
    ejes[2].axhline(objetivo, color=TINTA_2, linewidth=1, linestyle=":")
    ejes[2].annotate(f"objetivo {objetivo} %", (muestras["segundo"].max(), objetivo),
                     ha="right", va="bottom", color=TINTA_2, fontsize=9)
    ejes[2].set(title="CPU media por pod vista por el HPA (% del request)", ylabel="%",
                xlabel="Segundos desde el inicio de la carga (zona gris: carga activa)")
    for eje in ejes:
        eje.axvspan(0, fin_carga, color=REJILLA, alpha=0.45, linewidth=0)
    fig.tight_layout()
    fig.savefig(FIGURAS / "04_hpa_escalado.png", dpi=160)
    plt.close(fig)


def rolling() -> None:
    ruta = REPORTES / "rolling_update.json"
    if not ruta.exists():
        return
    resumen = json.loads(ruta.read_text(encoding="utf-8"))
    carga = historia("rolling")
    fig, (eje1, eje2) = plt.subplots(2, 1, figsize=(10, 5.5), sharex=True)
    eje1.plot(carga["segundo"], carga["Requests/s"], color=AZUL, linewidth=2)
    eje1.set(title="Solicitudes por segundo durante la actualización", ylabel="sol./s")
    eje2.plot(carga["segundo"], carga["Failures/s"], color=NARANJA, linewidth=2)
    eje2.set(title="Errores por segundo", ylabel="errores/s", xlabel="Segundos de carga",
             ylim=(0, max(1, carga["Failures/s"].max() * 1.2)))
    marcas = [
        (resumen["upgrade_inicio_segundo"], resumen["upgrade_segundos"],
         f"helm upgrade → {resumen['version_tras_upgrade']}"),
        (resumen["rollback_inicio_segundo"], resumen["rollback_segundos"],
         f"helm rollback → {resumen['version_tras_rollback']}"),
    ]
    for inicio, duracion, texto in marcas:
        for eje in (eje1, eje2):
            eje.axvspan(inicio, inicio + duracion, color=REJILLA, alpha=0.6, linewidth=0)
        eje1.annotate(texto, (inicio, carga["Requests/s"].max() * 0.1), va="bottom", ha="left",
                      xytext=(4, 0), textcoords="offset points", color=TINTA_2, fontsize=9)
    fig.tight_layout()
    fig.savefig(FIGURAS / "05_rolling_update.png", dpi=160)
    plt.close(fig)


def main() -> None:
    estilo()
    FIGURAS.mkdir(parents=True, exist_ok=True)
    ruta = REPORTES / "carga_cluster.json"
    if ruta.exists():
        resumen = json.loads(ruta.read_text(encoding="utf-8"))
        comparacion(resumen)
        escalado(resumen)
    rolling()


if __name__ == "__main__":
    main()
