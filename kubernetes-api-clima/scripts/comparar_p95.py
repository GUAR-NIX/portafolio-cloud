"""Latencias desde el histograma de Prometheus para cada escenario de carga.

1. p95_prometheus_vs_locust.csv: misma ventana que Locust (sin la rampa de 15 s) para comprobar que
   las cubetas del histograma bastan.
2. carga_cluster_regimen.csv: solo desde el segundo 60 de carga, cuando el HPA ya terminó de escalar.
   Hace falta porque los percentiles de stats_history de Locust son acumulados desde el inicio.

Requiere el port-forward a Prometheus en localhost:9090.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import requests

RAIZ = Path(__file__).resolve().parents[1]
PROMETHEUS = "http://localhost:9090/api/v1/query"
FILTRO = 'service="clima-ecuador-api", handler=~"/clima|/ciudades|/indicadores/.*"'


def consultar(expresion: str, instante: int) -> float | None:
    respuesta = requests.get(PROMETHEUS, params={"query": expresion, "time": instante}, timeout=30)
    respuesta.raise_for_status()
    resultado = respuesta.json()["data"]["result"]
    return float(resultado[0]["value"][1]) if resultado else None


def cuantil(q: float, desde: int, hasta: int) -> int | None:
    valor = consultar(
        f"histogram_quantile({q}, sum by (le) "
        f"(increase(http_request_duration_seconds_bucket{{{FILTRO}}}[{hasta - desde}s])))", hasta)
    return round(valor * 1000) if valor is not None else None


def regimen(resumen: dict) -> None:
    filas = []
    for escenario in resumen["escenarios"]:
        nombre = escenario["escenario"]
        historia = pd.read_csv(RAIZ / f"reports/carga_cluster_{nombre}_stats_history.csv")
        historia = historia[historia["Name"] == "Aggregated"]
        desde = escenario["inicio_carga_unix_nodo"] + 60
        hasta = escenario["fin_carga_unix_nodo"]
        estable = historia[(historia["Timestamp"] >= desde) & (historia["Timestamp"] <= hasta)]
        total = estable["Total Request Count"]
        fallos = estable["Total Failure Count"]
        filas.append({
            "escenario": nombre,
            "segundos": hasta - desde,
            "solicitudes_por_segundo_locust": round(
                float((total.iloc[-1] - total.iloc[0]) / (hasta - desde)), 1),
            "errores_locust": int(fallos.iloc[-1] - fallos.iloc[0]),
            "p50_ms_prometheus": cuantil(0.50, desde, hasta),
            "p95_ms_prometheus": cuantil(0.95, desde, hasta),
            "p99_ms_prometheus": cuantil(0.99, desde, hasta),
        })
    with (RAIZ / "reports/carga_cluster_regimen.csv").open(
        "w", newline="", encoding="utf-8"
    ) as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=list(filas[0]))
        escritor.writeheader()
        escritor.writerows(filas)
    for fila in filas:
        print(fila)


def main() -> None:
    resumen = json.loads((RAIZ / "reports/carga_cluster.json").read_text(encoding="utf-8"))
    filas = []
    for escenario in resumen["escenarios"]:
        # Se recortan 15 s al inicio (rampa) y se evalúa al final de la ventana de carga.
        inicio = escenario["inicio_carga_unix_nodo"] + 15
        fin = escenario["fin_carga_unix_nodo"]
        ventana = f"{fin - inicio}s"
        p95 = consultar(
            f"histogram_quantile(0.95, sum by (le) "
            f"(increase(http_request_duration_seconds_bucket{{{FILTRO}}}[{ventana}])))", fin)
        p50 = consultar(
            f"histogram_quantile(0.50, sum by (le) "
            f"(increase(http_request_duration_seconds_bucket{{{FILTRO}}}[{ventana}])))", fin)
        filas.append({
            "escenario": escenario["escenario"],
            "locust_p50_ms": escenario["latencia_p50_ms"],
            "prometheus_p50_ms": round(p50 * 1000) if p50 is not None else None,
            "locust_p95_ms": escenario["latencia_p95_ms"],
            "prometheus_p95_ms": round(p95 * 1000) if p95 is not None else None,
        })
    with (RAIZ / "reports/p95_prometheus_vs_locust.csv").open(
        "w", newline="", encoding="utf-8"
    ) as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=list(filas[0]))
        escritor.writeheader()
        escritor.writerows(filas)
    for fila in filas:
        print(fila)
    regimen(resumen)


if __name__ == "__main__":
    main()
