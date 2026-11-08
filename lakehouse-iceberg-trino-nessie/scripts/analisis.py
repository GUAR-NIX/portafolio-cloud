"""Análisis de negocio sobre los agregados + figuras para el README (reports/negocio/ y reports/figures/)."""

import csv
import json
import sys
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from lakehouse.catalogo import trino  # noqa: E402
from lakehouse.consultas import filas_dict, guardar  # noqa: E402

NEG, FIG = RAIZ / "reports" / "negocio", RAIZ / "reports" / "figures"
FINAL = datetime(2026, 7, 19, 19)  # inicio de la final (UTC)
COLOR = {"en": "#2a78d6", "es": "#eb6834"}
TINTA, TINTA2, SUPERFICIE = "#0b0b0b", "#52514e", "#fcfcfb"


def a_csv(nombre: str, filas: list[dict]) -> None:
    NEG.mkdir(parents=True, exist_ok=True)
    with (NEG / f"{nombre}.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0]))
        w.writeheader()
        w.writerows(filas)


def estilo(plt) -> None:
    plt.rcParams.update({
        "figure.facecolor": SUPERFICIE, "axes.facecolor": SUPERFICIE, "axes.edgecolor": "#c9c8c2",
        "axes.labelcolor": TINTA2, "xtick.color": TINTA2, "ytick.color": TINTA2, "text.color": TINTA,
        "axes.grid": True, "grid.color": "#e6e5e0", "grid.linewidth": 0.8, "axes.spines.top": False,
        "axes.spines.right": False, "font.size": 10, "axes.titleweight": "bold", "axes.titlesize": 12,
        "lines.linewidth": 2, "legend.frameon": False, "axes.axisbelow": True})


def consultas(con) -> dict:
    q = {}
    q["vistas_hora"] = filas_dict(con, """
        SELECT hora, idioma, sum(vistas) AS vistas, sum(CASE WHEN plataforma = 'movil' THEN vistas END) AS movil
        FROM iceberg.wiki.agg_vistas_hora GROUP BY 1, 2 ORDER BY 1, 2""")
    q["resumen_idioma"] = filas_dict(con, """
        SELECT idioma, CAST(hora AS date) AS dia, sum(vistas) AS vistas,
               round(100.0 * sum(CASE WHEN plataforma = 'movil' THEN vistas END) / sum(vistas), 1) AS pct_movil
        FROM iceberg.wiki.agg_vistas_hora GROUP BY 1, 2 ORDER BY 1, 2""")
    q["top10_dia_final"] = filas_dict(con, """
        SELECT idioma, puesto, titulo, vistas FROM iceberg.wiki.agg_top_dia
        WHERE dia = DATE '2026-07-19' AND puesto <= 10 ORDER BY idioma, puesto""")
    q["tendencia_19_vs_18"] = filas_dict(con, """
        WITH d AS (SELECT idioma, titulo, CAST(hora AS date) AS dia, sum(vistas) AS v
                   FROM iceberg.wiki.agg_articulo_hora GROUP BY 1, 2, 3),
             p AS (SELECT idioma, titulo, sum(CASE WHEN dia = DATE '2026-07-18' THEN v ELSE 0 END) AS v18,
                          sum(CASE WHEN dia = DATE '2026-07-19' THEN v ELSE 0 END) AS v19
                   FROM d GROUP BY 1, 2)
        SELECT * FROM (SELECT idioma, titulo, v18, v19, v19 - v18 AS aumento,
                              row_number() OVER (PARTITION BY idioma ORDER BY v19 - v18 DESC) AS puesto FROM p)
        WHERE puesto <= 10 ORDER BY idioma, puesto""")
    q["picos_top"] = filas_dict(con, """
        SELECT * FROM (SELECT p.*, row_number() OVER (PARTITION BY idioma, titulo ORDER BY razon DESC) AS r
                       FROM iceberg.wiki.agg_picos p) WHERE r = 1 ORDER BY razon DESC LIMIT 20""")
    q["picos_por_hora"] = filas_dict(con, """
        SELECT hora, idioma, count(*) AS articulos_en_pico FROM iceberg.wiki.agg_picos GROUP BY 1, 2 ORDER BY 1, 2""")
    q["comunes_top50_final"] = filas_dict(con, """
        SELECT en.titulo, en.puesto AS puesto_en, es.puesto AS puesto_es, en.vistas AS vistas_en, es.vistas AS vistas_es
        FROM iceberg.wiki.agg_top_dia en JOIN iceberg.wiki.agg_top_dia es
          ON en.titulo = es.titulo AND en.dia = es.dia AND en.idioma = 'en' AND es.idioma = 'es'
        WHERE en.dia = DATE '2026-07-19' ORDER BY en.puesto""")
    q["articulos_final"] = filas_dict(con, """
        SELECT idioma, titulo, sum(vistas) AS vistas FROM iceberg.wiki.agg_articulo_hora
        WHERE (idioma = 'en' AND titulo = '2026_FIFA_World_Cup_final')
           OR (idioma = 'es' AND titulo LIKE 'Final_de_la_Copa_Mundial_de_F%2026')
        GROUP BY 1, 2""")
    return q


def figuras(q: dict) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    estilo(plt)
    FIG.mkdir(parents=True, exist_ok=True)
    fmt = mdates.DateFormatter("%d-jul\n%H h")

    # 1) Vistas por hora e idioma (millones), con la final marcada
    fig, ax = plt.subplots(figsize=(10, 4.2))
    for idioma in ("en", "es"):
        s = [r for r in q["vistas_hora"] if r["idioma"] == idioma]
        ax.plot([r["hora"] for r in s], [r["vistas"] / 1e6 for r in s], color=COLOR[idioma])
        ax.annotate("inglés" if idioma == "en" else "español", (s[-1]["hora"], s[-1]["vistas"] / 1e6),
                    xytext=(6, 0), textcoords="offset points", color=TINTA2, va="center")
    ax.axvline(FINAL, color=TINTA2, linewidth=1, linestyle="--")
    ax.text(FINAL, ax.get_ylim()[1] * 0.97, " inicio de la final (19:00 UTC)", color=TINTA2, va="top")
    ax.set_title("Vistas por hora en Wikipedia (artículos con ≥ 5 vistas por hora)")
    ax.set_ylabel("millones de vistas por hora")
    ax.xaxis.set_major_formatter(fmt)
    fig.tight_layout()
    fig.savefig(FIG / "01_vistas_por_hora.png", dpi=130)
    plt.close(fig)

    # 2) Cuántos artículos están en pico cada hora (≥ 5× su promedio de las 24 h previas)
    fig, ax = plt.subplots(figsize=(10, 3.6))
    # serie densa: las horas sin ningún pico cuentan 0 (la base de 24 h solo existe desde el 19-jul 00 h)
    horas = sorted({r["hora"] for r in q["vistas_hora"]})[24:]
    for idioma in ("en", "es"):
        n = {r["hora"]: r["articulos_en_pico"] for r in q["picos_por_hora"] if r["idioma"] == idioma}
        ax.plot(horas, [n.get(h, 0) for h in horas], color=COLOR[idioma],
                label="inglés" if idioma == "en" else "español")
    ax.axvline(FINAL, color=TINTA2, linewidth=1, linestyle="--")
    ax.set_title("Artículos en pico por hora (≥ 5 000 vistas y ≥ 5× su base de 24 h)")
    ax.set_ylabel("artículos")
    ax.legend(loc="upper left")
    ax.xaxis.set_major_formatter(fmt)
    fig.tight_layout()
    fig.savefig(FIG / "02_picos_por_hora.png", dpi=130)
    plt.close(fig)

    # 3) Top 10 del día de la final, un panel por idioma (mismo eje x en miles)
    fig, axs = plt.subplots(1, 2, figsize=(11, 4.4))
    for ax, idioma in zip(axs, ("en", "es"), strict=True):
        s = [r for r in q["top10_dia_final"] if r["idioma"] == idioma][::-1]
        ax.barh([r["titulo"].replace("_", " ")[:38] for r in s], [r["vistas"] / 1e3 for r in s],
                color=COLOR[idioma], height=0.7)
        ax.set_title(("Inglés" if idioma == "en" else "Español") + " · 19-jul-2026")
        ax.set_xlabel("miles de vistas en el día")
        ax.grid(axis="y", visible=False)
    fig.tight_layout()
    fig.savefig(FIG / "03_top10_dia_final.png", dpi=130)
    plt.close(fig)


def figuras_tecnicas() -> None:
    """Rendimiento y compactación (a partir de los JSON de reports/)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    estilo(plt)
    rend = json.loads((RAIZ / "reports" / "rendimiento.json").read_text(encoding="utf-8"))["casos"]
    nombres = list(rend)
    fig, axs = plt.subplots(1, 2, figsize=(11, 3.8))
    for ax, clave, titulo in ((axs[0], "mb_leidos", "MB leídos (mediana de 5)"),
                              (axs[1], "ms_mediana", "milisegundos (mediana de 5)")):
        vals = [rend[n][clave] for n in nombres][::-1]
        barras = ax.barh([n.replace(" · ", "\n") for n in nombres][::-1], vals, color="#2a78d6", height=0.6)
        ax.bar_label(barras, labels=[f"{v:,.1f}" if clave == "mb_leidos" else f"{v:,.0f}" for v in vals],
                     padding=3, color=TINTA2)
        ax.set_title(titulo)
        ax.grid(axis="y", visible=False)
    axs[1].set_yticklabels([])
    fig.tight_layout()
    fig.savefig(FIG / "04_rendimiento_poda.png", dpi=130)
    plt.close(fig)

    comp = json.loads((RAIZ / "reports" / "iceberg" / "04_compactacion.json").read_text(encoding="utf-8"))
    etapas = [("antes", "antes"), ("despues_optimize", "tras optimize"), ("despues_nessie_gc", "tras nessie-gc")]
    fig, axs = plt.subplots(1, 2, figsize=(10, 3.4))
    for ax, f, titulo in ((axs[0], lambda e: comp[e]["minio"]["data"]["objetos"], "Archivos de datos en MinIO"),
                          (axs[1], lambda e: comp[e]["minio"]["data"]["bytes"] / 1e6, "MB de datos en MinIO")):
        vals = [f(e) for e, _ in etapas]
        barras = ax.bar([n for _, n in etapas], vals, color="#2a78d6", width=0.55)
        ax.bar_label(barras, labels=[f"{v:,.0f}" for v in vals], padding=3, color=TINTA2)
        ax.set_title(titulo)
        ax.grid(axis="x", visible=False)
    fig.tight_layout()
    fig.savefig(FIG / "05_compactacion.png", dpi=130)
    plt.close(fig)


def main() -> None:
    if sys.argv[1:] == ["tecnicas"]:
        figuras_tecnicas()
        return
    q = consultas(trino())
    for nombre in ("resumen_idioma", "top10_dia_final", "tendencia_19_vs_18", "picos_top", "comunes_top50_final"):
        a_csv(nombre, q[nombre])
    guardar(NEG / "consultas.json", {k: v for k, v in q.items() if k not in ("vistas_hora", "picos_por_hora")})
    figuras(q)
    for k in ("resumen_idioma", "articulos_final"):
        print(k, q[k])


if __name__ == "__main__":
    main()
