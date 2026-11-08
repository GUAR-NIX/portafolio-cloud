"""Acceso de solo lectura al Parquet incluido en la imagen."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd


class RepositorioClima:
    def __init__(self, ruta: Path):
        self.ruta = ruta

    @lru_cache(maxsize=1)
    def datos(self) -> pd.DataFrame:
        marco = pd.read_parquet(self.ruta)
        marco["fecha"] = pd.to_datetime(marco["fecha"]).dt.date
        return marco

    def listo(self) -> bool:
        try:
            return self.ruta.is_file() and not self.datos().empty
        except (OSError, ValueError):
            return False

    def ciudades(self) -> list[str]:
        return sorted(self.datos()["ciudad"].unique().tolist())

    def por_ciudad_fecha(self, ciudad: str, fecha: str) -> dict | None:
        objetivo = pd.to_datetime(fecha).date()
        filas = self.datos()
        fila = filas[(filas["ciudad"].str.casefold() == ciudad.casefold()) & (filas["fecha"] == objetivo)]
        if fila.empty:
            return None
        return fila.iloc[0].to_dict()

    def indicadores(self, ciudad: str) -> dict | None:
        filas = self.datos()[self.datos()["ciudad"].str.casefold() == ciudad.casefold()]
        if filas.empty:
            return None
        return {
            "ciudad": filas.iloc[0]["ciudad"],
            "desde": filas["fecha"].min().isoformat(),
            "hasta": filas["fecha"].max().isoformat(),
            "dias": int(len(filas)),
            "temperatura_media_c": round(float(filas["temperatura_media_c"].mean()), 2),
            "temperatura_maxima_c": round(float(filas["temperatura_maxima_c"].max()), 2),
            "temperatura_minima_c": round(float(filas["temperatura_minima_c"].min()), 2),
            "precipitacion_total_mm": round(float(filas["precipitacion_mm"].sum()), 2),
            "dias_lluviosos": int((filas["precipitacion_mm"] >= 1).sum()),
        }

