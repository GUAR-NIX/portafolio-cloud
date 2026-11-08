from __future__ import annotations

import os
from pathlib import Path


def ruta_datos() -> Path:
    predeterminada = Path(__file__).resolve().parents[2] / "data/procesados/clima_ecuador.parquet"
    return Path(os.getenv("RUTA_DATOS", predeterminada))

