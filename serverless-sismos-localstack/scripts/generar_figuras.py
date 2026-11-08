"""Genera figuras del README a partir de datos y reportes reales."""

from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from math import asin, atan2, cos, degrees, radians, sin
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))
from serverless_sismos.geografia import CENTRO_ECUADOR, RADIO_TIERRA_KM  # noqa: E402

AZUL, NARANJA, ROJO = "#2a78d6", "#eb6834", "#e34948"
TEXTO, TEXTO_2, GRIS = "#0b0b0b", "#52514e", "#b9b8b2"


def _guardar(fig, nombre: str) -> None:
    fig.tight_layout()
    fig.savefig(RAIZ / "reports/figures" / nombre, dpi=160)
    plt.close(fig)


def figura_latencias(lat: dict) -> None:
    calientes = [m for m in lat["mediciones"] if m["arranque"] == "caliente"]
    series = [
        ("S3 → DynamoDB", "s3_a_dynamodb", AZUL),
        ("S3 → alerta\n(SNS → SQS)", "s3_a_alerta", NARANJA),
    ]
    fig, eje = plt.subplots(figsize=(9, 4.2))
    tope = max(lat["caliente"][clave]["max_ms"] for _, clave, _ in series)
    for posicion, (_, clave, color) in enumerate(series):
        valores = [m[f"{clave}_ms"] for m in calientes]
        resumen = lat["caliente"][clave]
        eje.boxplot(
            valores,
            positions=[posicion],
            widths=0.4,
            vert=False,
            showfliers=False,
            medianprops={"color": TEXTO, "linewidth": 2},
            boxprops={"color": TEXTO_2},
            whiskerprops={"color": TEXTO_2},
            capprops={"color": TEXTO_2},
        )
        eje.scatter(
            valores, [posicion + 0.3] * len(valores), s=24, color=color, alpha=0.85,
            edgecolor="white", linewidth=0.6, zorder=3,
        )
        eje.annotate(
            f"p50 {resumen['p50_ms']:.0f} ms · p95 {resumen['p95_ms']:.0f} ms",
            (tope, posicion), xytext=(10, 0), textcoords="offset points",
            va="center", color=TEXTO_2, fontsize=9,
        )
    eje.set_yticks([0, 1], [s[0] for s in series])
    eje.set_xlim(0, tope * 1.5)
    eje.set(
        xlabel="Milisegundos desde put_object en S3",
        title=f"Latencia de extremo a extremo con Lambda caliente (n={len(calientes)})",
    )
    _guardar(fig, "02_latencias_e2e.png")


def figura_frio_caliente(lat: dict) -> None:
    fig, ejes = plt.subplots(1, 2, figsize=(10, 4.2), sharey=True)
    for eje, (clave, titulo) in zip(
        ejes, [("s3_a_dynamodb", "S3 → DynamoDB"), ("s3_a_alerta", "S3 → alerta")]
    ):
        for paso, (tipo, color, nombre) in enumerate(
            [("frio", NARANJA, "Frío"), ("caliente", AZUL, "Caliente")]
        ):
            r = lat[tipo][clave]
            barras = eje.bar(
                [paso * 0.38, 1 + paso * 0.38], [r["p50_ms"], r["p95_ms"]], width=0.36,
                color=color, label=f"{nombre} (n={r['n']})", edgecolor="white", linewidth=2,
            )
            eje.bar_label(barras, fmt="%.0f", padding=3, color=TEXTO_2, fontsize=9)
        eje.set_xticks([0.19, 1.19], ["p50", "p95"])
        eje.set_title(titulo)
    ejes[0].set_ylabel("Milisegundos")
    ejes[0].legend(frameon=False)
    fig.suptitle("Arranque en frío frente a caliente (Lambda en LocalStack)", color=TEXTO)
    _guardar(fig, "03_frio_vs_caliente.png")


def figura_flujo(vol: dict, cua: dict) -> None:
    filas = [
        ("Feed semanal · 1.ª entrega", vol["primera_entrega"]),
        ("Feed semanal · 2.ª entrega idéntica", vol["segunda_entrega_identica"]),
        ("Ecuador 2024 + 5 errores", cua["resumen_lambda"]),
    ]
    tramos = [
        ("insertados", AZUL, "Insertados"),
        ("duplicados", GRIS, "Duplicados evitados"),
        ("invalidos", NARANJA, "A cuarentena"),
    ]
    fig, eje = plt.subplots(figsize=(10, 3.8))
    usados = set()
    for y, (_, r) in enumerate(filas):
        izquierda = 0
        for campo, color, etiqueta in tramos:
            if r[campo]:
                eje.barh(
                    y, r[campo], left=izquierda, color=color, height=0.55, edgecolor="white",
                    linewidth=2, label=None if etiqueta in usados else etiqueta,
                )
                usados.add(etiqueta)
                izquierda += r[campo]
        texto = (
            f"{r['insertados']:,} ins. · {r['duplicados']:,} dup. · "
            f"{r['invalidos']} cuar. · {r['alertas']} alertas"
        ).replace(",", ".")
        eje.text(izquierda + 25, y, texto, va="center", color=TEXTO_2, fontsize=9)
    eje.set_yticks(range(len(filas)), [f[0] for f in filas])
    eje.invert_yaxis()
    eje.set_xlim(0, max(f[1]["recibidos"] for f in filas) * 1.6)
    eje.set(
        xlabel="Eventos del archivo",
        title="Destino de cada evento según la Lambda (conteos de CloudWatch Logs)",
    )
    eje.legend(frameon=False, loc="lower right")
    _guardar(fig, "04_flujo_eventos.png")


def _circulo(radio_km: float) -> tuple[list[float], list[float]]:
    """Puntos a distancia fija del centro de Ecuador (fórmula de destino esférico)."""
    lat0, lon0 = radians(CENTRO_ECUADOR[0]), radians(CENTRO_ECUADOR[1])
    d = radio_km / RADIO_TIERRA_KM
    lons, lats = [], []
    for grado in range(0, 361, 3):
        rumbo = radians(grado)
        lat = asin(sin(lat0) * cos(d) + cos(lat0) * sin(d) * cos(rumbo))
        lon = lon0 + atan2(sin(rumbo) * sin(d) * cos(lat0), cos(d) - sin(lat0) * sin(lat))
        lons.append(degrees(lon))
        lats.append(degrees(lat))
    return lons, lats


def figura_mapa(semana: dict, ecuador: dict, alertas: list[dict]) -> None:
    eventos = semana["features"] + ecuador["features"]
    ids = {a["id_evento"] for a in alertas}
    alertados = [e for e in eventos if e.get("id") in ids]
    lon = [e["geometry"]["coordinates"][0] for e in eventos]
    lat = [e["geometry"]["coordinates"][1] for e in eventos]
    mag = [max(e["properties"]["mag"] or 0, 0) for e in eventos]
    fig, ejes = plt.subplots(1, 2, figsize=(12, 5), gridspec_kw={"width_ratios": [1.6, 1]})
    for eje, zoom in zip(ejes, [False, True]):
        eje.scatter(
            lon, lat, s=[2 + m**2 for m in mag], color=AZUL, alpha=0.35, linewidth=0,
            label="Sismos recibidos (tamaño según magnitud)",
        )
        eje.plot(
            *_circulo(1500), color=TEXTO_2, linewidth=1.2, linestyle="--",
            label="1.500 km del centro de Ecuador",
        )
        eje.scatter(
            [e["geometry"]["coordinates"][0] for e in alertados],
            [e["geometry"]["coordinates"][1] for e in alertados],
            marker="*", s=200, color=ROJO, edgecolor="white", linewidth=0.8, zorder=4,
            label=f"Alertas emitidas ({len(alertados)})",
        )
        if zoom:
            eje.set(xlim=(-97, -60), ylim=(-17, 13), title="Zona de alerta (M ≥ 5)")
            for e in alertados:
                x, y, _ = e["geometry"]["coordinates"]
                eje.annotate(
                    f"M{e['properties']['mag']}", (x, y), xytext=(6, 4),
                    textcoords="offset points", fontsize=8, color=TEXTO,
                )
        else:
            eje.set(xlim=(-180, 180), ylim=(-75, 85), title="Feed semanal USGS + FDSN Ecuador 2024")
            eje.legend(frameon=False, loc="lower left", fontsize=8)
        eje.set(xlabel="Longitud", ylabel="Latitud")
        eje.set_aspect("equal")
    _guardar(fig, "05_mapa_alertas.png")


def figura_serie(lat: dict) -> None:
    filas = lat["mediciones"]
    x = list(range(1, len(filas) + 1))
    fig, eje = plt.subplots(figsize=(10, 4))
    eje.plot(x, [m["s3_a_alerta_ms"] for m in filas], color=NARANJA, linewidth=2, label="S3 → alerta")
    eje.plot(
        x, [m["s3_a_dynamodb_ms"] for m in filas], color=AZUL, linewidth=2, label="S3 → DynamoDB"
    )
    frios = [i for i, m in zip(x, filas) if m["arranque"] == "frio"]
    eje.scatter(
        frios, [filas[i - 1]["s3_a_alerta_ms"] for i in frios], s=64, color=NARANJA,
        edgecolor="white", linewidth=2, zorder=3, label="Arranque en frío forzado",
    )
    eje.set_yscale("log")
    eje.set(
        xlabel="Subida n.º",
        ylabel="Milisegundos (escala log)",
        title="Latencia por subida: los picos son entornos Lambda nuevos",
    )
    eje.legend(frameon=False)
    _guardar(fig, "06_latencia_por_subida.png")


def main() -> None:
    raiz = RAIZ
    datos = json.loads((raiz / "data/raw/usgs_semana.geojson").read_text(encoding="utf-8"))
    magnitudes = [f["properties"]["mag"] for f in datos["features"] if f["properties"]["mag"] is not None]
    horas = Counter(
        datetime.fromtimestamp(f["properties"]["time"] / 1000, tz=timezone.utc).hour
        for f in datos["features"]
    )
    salida = raiz / "reports/figures"
    salida.mkdir(parents=True, exist_ok=True)
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ejes = plt.subplots(1, 2, figsize=(11, 4.2))
    ejes[0].hist(magnitudes, bins=20, color="#2563eb", edgecolor="white")
    ejes[0].set(title="Magnitudes del feed semanal USGS", xlabel="Magnitud", ylabel="Eventos")
    ejes[1].bar(range(24), [horas[h] for h in range(24)], color="#0f766e")
    ejes[1].set(title="Eventos por hora UTC", xlabel="Hora UTC", ylabel="Eventos", xticks=range(0, 24, 3))
    fig.tight_layout()
    fig.savefig(salida / "01_feed_usgs.png", dpi=160)
    plt.close(fig)

    integracion = raiz / "reports/integracion_localstack.json"
    if not integracion.exists():
        print("Falta reports/integracion_localstack.json: ejecute scripts/medir_integracion.py")
        return
    medida = json.loads(integracion.read_text(encoding="utf-8"))
    ecuador = json.loads((raiz / "data/raw/usgs_ecuador_2024.geojson").read_text(encoding="utf-8"))
    vol, cua, lat = medida["volumen_y_duplicados"], medida["cuarentena"], medida["latencias"]
    figura_latencias(lat)
    figura_frio_caliente(lat)
    figura_flujo(vol, cua)
    figura_mapa(datos, ecuador, vol["alertas"] + cua["alertas"])
    figura_serie(lat)


if __name__ == "__main__":
    main()
