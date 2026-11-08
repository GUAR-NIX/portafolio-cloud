"""Descarga reanudable de feeds USGS y una consulta histórica reproducible."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

import requests

FEEDS = {
    "hora": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
    "dia": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
    "semana": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson",
}
URL_HISTORICO = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def descargar(url: str, destino: Path, parametros: dict[str, object] | None = None) -> dict:
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_suffix(destino.suffix + ".parcial")
    respuesta = requests.get(url, params=parametros, timeout=90)
    respuesta.raise_for_status()
    temporal.write_bytes(respuesta.content)
    json.loads(temporal.read_text(encoding="utf-8"))
    temporal.replace(destino)
    return {
        "archivo": str(destino),
        "url": respuesta.url,
        "bytes": destino.stat().st_size,
        "sha256": hashlib.sha256(destino.read_bytes()).hexdigest(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--salida", type=Path, default=Path("data/raw"))
    parser.add_argument("--forzar", action="store_true")
    args = parser.parse_args()
    manifiesto = []
    for nombre, url in FEEDS.items():
        destino = args.salida / f"usgs_{nombre}.geojson"
        if destino.exists() and not args.forzar:
            manifiesto.append({"archivo": str(destino), "estado": "cache"})
        else:
            manifiesto.append(descargar(url, destino))
    historico = args.salida / "usgs_ecuador_2024.geojson"
    if not historico.exists() or args.forzar:
        manifiesto.append(
            descargar(
                URL_HISTORICO,
                historico,
                {
                    "format": "geojson",
                    "starttime": "2024-01-01",
                    "endtime": "2025-01-01",
                    "minlatitude": -6,
                    "maxlatitude": 3,
                    "minlongitude": -83,
                    "maxlongitude": -74,
                    "minmagnitude": 2.5,
                    "orderby": "time-asc",
                },
            )
        )
    (args.salida / "manifiesto_descarga.json").write_text(
        json.dumps(
            {"fecha_descarga": date.today().isoformat(), "archivos": manifiesto},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps(manifiesto, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

