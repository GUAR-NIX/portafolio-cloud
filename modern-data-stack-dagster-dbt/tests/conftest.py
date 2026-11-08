import gzip
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]


def escribir_csv_gz(ruta: Path, filas: list[str]) -> Path:
    with gzip.open(ruta, "wt", encoding="utf-8") as f:
        f.write("\n".join(filas) + "\n")
    return ruta


@pytest.fixture
def csv_valido(tmp_path) -> Path:
    """Un año sintético con el formato exacto de NOAA (sin cabecera, banderas vacías)."""
    filas = []
    for dia in range(1, 29):
        fecha = f"202402{dia:02d}"
        for est in ("EC000084370", "SPE00120287", "USW00094728"):
            filas += [f"{est},{fecha},TMAX,{250 + dia},,,S,", f"{est},{fecha},TMIN,{120 + dia},,,S,",
                      f"{est},{fecha},PRCP,{dia % 5 * 10},,,S,0700", f"{est},{fecha},SNWD,0,,,S,"]
    filas.append("USW00094728,20240229,TMAX,9999,,X,S,")  # con QFLAG: NOAA ya lo marcó, no cuenta
    return escribir_csv_gz(tmp_path / "2024.csv.gz", filas)
