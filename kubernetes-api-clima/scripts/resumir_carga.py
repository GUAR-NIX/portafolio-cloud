"""Convierte las salidas de Locust en un JSON y una figura reproducibles."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main() -> None:
    raiz = Path(__file__).resolve().parents[1]
    estadisticas = pd.read_csv(raiz / "reports/carga_local_stats.csv")
    historia = pd.read_csv(raiz / "reports/carga_local_stats_history.csv")
    total = estadisticas[estadisticas["Name"] == "Aggregated"].iloc[0]
    resultado = {
        "entorno": "Uvicorn con un proceso, sin contenedor",
        "usuarios_concurrentes": 50,
        "duracion_segundos": 30,
        "solicitudes": int(total["Request Count"]),
        "fallos": int(total["Failure Count"]),
        "solicitudes_por_segundo": round(float(total["Requests/s"]), 2),
        "latencia_mediana_ms": int(total["50%"]),
        "latencia_p95_ms": int(total["95%"]),
        "latencia_maxima_ms": round(float(total["Max Response Time"]), 2),
        "hpa_verificado": False,
        "motivo_hpa_no_verificado": "Docker Desktop no levantó el motor en esta ejecución.",
    }
    (raiz / "reports/carga_local.json").write_text(
        json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    serie = historia[(historia["Name"] == "Aggregated") & (historia["User Count"] > 0)].copy()
    serie["segundo"] = serie["Timestamp"] - serie["Timestamp"].min()
    fig, eje1 = plt.subplots(figsize=(10, 4.5))
    eje1.plot(serie["segundo"], serie["Requests/s"], color="#2563eb", label="Solicitudes/s")
    eje1.set(xlabel="Segundo", ylabel="Solicitudes/s", title="Carga local de una instancia")
    eje2 = eje1.twinx()
    eje2.plot(serie["segundo"], serie["95%"], color="#dc2626", label="Latencia p95")
    eje2.set_ylabel("p95 (ms)")
    fig.tight_layout()
    fig.savefig(raiz / "reports/figures/02_carga_local.png", dpi=160)
    plt.close(fig)
    print(json.dumps(resultado, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

