"""Reproduce la lógica completa en memoria y guarda métricas medibles."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path


def ejecutar(documento: dict) -> tuple[dict, list[float]]:
    from serverless_sismos.procesamiento import procesar_geojson

    almacen: dict[str, dict] = {}
    alertas: list[dict] = []

    def guardar(item: dict) -> bool:
        identificador = str(item["id_evento"])
        if identificador in almacen:
            return False
        almacen[identificador] = item
        return True

    latencias = []
    inicio = time.perf_counter()
    primera = procesar_geojson(documento, guardar, alertas.append)
    latencias.append((time.perf_counter() - inicio) * 1000)
    inicio = time.perf_counter()
    segunda = procesar_geojson(documento, guardar, alertas.append)
    latencias.append((time.perf_counter() - inicio) * 1000)
    return {
        "primera_carga": primera,
        "segunda_carga": segunda,
        "eventos_finales": len(almacen),
        "alertas_emitidas": len(alertas),
    }, latencias


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entrada", type=Path, default=Path("data/raw/usgs_semana.geojson"))
    parser.add_argument("--salida", type=Path, default=Path("reports/resultados.json"))
    args = parser.parse_args()
    documento = json.loads(args.entrada.read_text(encoding="utf-8"))
    resultado, latencias = ejecutar(documento)
    resultado["medicion_local"] = {
        "latencia_primera_carga_ms": round(latencias[0], 3),
        "latencia_repeticion_ms": round(latencias[1], 3),
        "latencia_mediana_ms": round(statistics.median(latencias), 3),
        "nota": "Lógica en proceso local; no incluye red ni arranque de Lambda.",
    }
    args.salida.parent.mkdir(parents=True, exist_ok=True)
    args.salida.write_text(json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(resultado, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

