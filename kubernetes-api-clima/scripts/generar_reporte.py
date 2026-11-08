"""Resume el Parquet y genera la figura usada en el README."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main() -> None:
    raiz = Path(__file__).resolve().parents[1]
    datos = pd.read_parquet(raiz / "data/procesados/clima_ecuador.parquet")
    resumen = (
        datos.groupby("ciudad")
        .agg(
            dias=("fecha", "count"),
            temperatura_media_c=("temperatura_media_c", "mean"),
            maxima_c=("temperatura_maxima_c", "max"),
            minima_c=("temperatura_minima_c", "min"),
            precipitacion_mm=("precipitacion_mm", "sum"),
        )
        .round(2)
        .sort_values("temperatura_media_c", ascending=False)
    )
    resultado = {
        "filas": int(len(datos)),
        "ciudades": int(datos["ciudad"].nunique()),
        "desde": str(datos["fecha"].min()),
        "hasta": str(datos["fecha"].max()),
        "nulos": int(datos.isna().sum().sum()),
        "duplicados": int(datos.duplicated(["ciudad", "fecha"]).sum()),
        "resumen_ciudades": resumen.reset_index().to_dict(orient="records"),
    }
    (raiz / "reports").mkdir(exist_ok=True)
    (raiz / "reports/resultados_datos.json").write_text(
        json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, eje = plt.subplots(figsize=(10, 5))
    resumen.sort_values("temperatura_media_c")["temperatura_media_c"].plot.barh(
        ax=eje, color="#2563eb"
    )
    eje.set(title="Temperatura media diaria en 2024", xlabel="°C", ylabel="Ciudad")
    fig.tight_layout()
    salida = raiz / "reports/figures"
    salida.mkdir(parents=True, exist_ok=True)
    fig.savefig(salida / "01_temperatura_media.png", dpi=160)
    plt.close(fig)
    print(json.dumps(resultado, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

