"""Descarga reanudable de NOAA GHCN-Daily (AWS Open Data) a data/raw/, fuera de Dagster.

Dagster hace lo mismo en el activo ghcn/ghcn_csv_anual; este script sirve para precargar la caché.
Uso:  python descargar_datos.py [2022 2023 ...]
"""

import sys

from clima_mds import configuracion as cfg
from clima_mds.descarga import descargar_reanudable

if __name__ == "__main__":
    anios = sys.argv[1:] or cfg.ANIOS
    for nombre in ("ghcnd-stations.txt", "ghcnd-countries.txt"):
        print(nombre, descargar_reanudable(f"{cfg.URL_BASE_NOAA}/{nombre}", cfg.DIR_CRUDO / nombre))
    for anio in anios:
        res = descargar_reanudable(f"{cfg.URL_BASE_NOAA}/csv.gz/by_year/{anio}.csv.gz",
                                   cfg.DIR_CRUDO / "by_year" / f"{anio}.csv.gz")
        print(anio, f"{res.bytes_totales / 1e6:.1f} MB", "caché" if res.reutilizado else f"{res.segundos:.0f} s")
