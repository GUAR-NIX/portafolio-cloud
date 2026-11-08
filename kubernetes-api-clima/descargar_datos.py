"""Descarga reanudable de clima diario de Open-Meteo para 12 ciudades."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

import pandas as pd
import requests

URL = "https://archive-api.open-meteo.com/v1/archive"
CIUDADES = {
    "Quito": (-0.1807, -78.4678),
    "Guayaquil": (-2.1709, -79.9224),
    "Cuenca": (-2.9001, -79.0059),
    "Loja": (-3.9931, -79.2042),
    "Ambato": (-1.2491, -78.6168),
    "Manta": (-0.9677, -80.7089),
    "Esmeraldas": (0.9592, -79.6539),
    "Ibarra": (0.3517, -78.1223),
    "Puyo": (-1.4924, -78.0026),
    "Nueva Loja": (0.0860, -76.8953),
    "Santo Domingo": (-0.2531, -79.1754),
    "Puerto Ayora": (-0.7433, -90.3138),
}


def descargar_ciudad(ciudad: str, latitud: float, longitud: float, destino: Path, forzar: bool) -> dict:
    destino.parent.mkdir(parents=True, exist_ok=True)
    if destino.exists() and not forzar:
        return json.loads(destino.read_text(encoding="utf-8"))
    parametros = {
        "latitude": latitud,
        "longitude": longitud,
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "America/Guayaquil",
    }
    respuesta = requests.get(URL, params=parametros, timeout=90)
    respuesta.raise_for_status()
    documento = respuesta.json()
    temporal = destino.with_suffix(".parcial")
    temporal.write_text(json.dumps(documento, ensure_ascii=False), encoding="utf-8")
    temporal.replace(destino)
    return documento


def transformar(ciudad: str, documento: dict) -> pd.DataFrame:
    diario = documento["daily"]
    return pd.DataFrame(
        {
            "ciudad": ciudad,
            "fecha": diario["time"],
            "temperatura_media_c": diario["temperature_2m_mean"],
            "temperatura_maxima_c": diario["temperature_2m_max"],
            "temperatura_minima_c": diario["temperature_2m_min"],
            "precipitacion_mm": diario["precipitation_sum"],
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--forzar", action="store_true")
    parser.add_argument("--raw", type=Path, default=Path("data/raw"))
    parser.add_argument("--salida", type=Path, default=Path("data/procesados/clima_ecuador.parquet"))
    args = parser.parse_args()
    marcos = []
    manifiesto = []
    for ciudad, (latitud, longitud) in CIUDADES.items():
        ruta = args.raw / f"{ciudad.lower().replace(' ', '_')}_2024.json"
        documento = descargar_ciudad(ciudad, latitud, longitud, ruta, args.forzar)
        marcos.append(transformar(ciudad, documento))
        manifiesto.append({"ciudad": ciudad, "archivo": str(ruta), "filas": len(documento["daily"]["time"])})
    datos = pd.concat(marcos, ignore_index=True)
    if datos.isna().any().any() or datos.duplicated(["ciudad", "fecha"]).any():
        raise ValueError("Los datos descargados no pasan los controles de calidad")
    args.salida.parent.mkdir(parents=True, exist_ok=True)
    datos.to_parquet(args.salida, index=False, compression="zstd")
    salida_manifiesto = {
        "fuente": URL,
        "licencia": "CC BY 4.0",
        "fecha_descarga": date.today().isoformat(),
        "periodo": [str(datos["fecha"].min()), str(datos["fecha"].max())],
        "filas": len(datos),
        "ciudades": len(CIUDADES),
        "sha256_parquet": hashlib.sha256(args.salida.read_bytes()).hexdigest(),
        "detalle": manifiesto,
    }
    (args.salida.parent / "manifiesto.json").write_text(
        json.dumps(salida_manifiesto, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(salida_manifiesto, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

