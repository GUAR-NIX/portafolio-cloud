"""Descarga directa de las fuentes oficiales a data/raw/ (JSON), sin Airflow.

Sirve para revisar los datos crudos sin levantar Docker o para analizarlos aparte. El pipeline NO depende de
este script: los DAG descargan por su cuenta, partición por partición.

Para ahorrar cuota de Open-Meteo pide un mes por petición (13 ciudades × ~31 días ≈ 13 × 3 llamadas), en
lugar de un día por petición como el DAG.

    .venv\\Scripts\\python scripts\\descargar_datos.py --desde 2025-10-01 --hasta 2026-10-14
    .venv\\Scripts\\python scripts\\descargar_datos.py --solo banco_mundial
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "include"))

from ingesta import apis, config  # noqa: E402

DESTINO = RAIZ / "data" / "raw"


def meses(desde: date, hasta: date):
    inicio = desde
    while inicio <= hasta:
        siguiente = (inicio.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield inicio, min(siguiente - timedelta(days=1), hasta)
        inicio = siguiente


def descargar_clima(desde: date, hasta: date) -> None:
    carpeta = DESTINO / "open_meteo"
    carpeta.mkdir(parents=True, exist_ok=True)
    for inicio, fin in meses(desde, hasta):
        archivo = carpeta / f"clima_{inicio:%Y-%m}.json"
        if archivo.exists():
            print(f"  ya existe {archivo.name}")
            continue
        params = apis.parametros_clima(inicio) | {"start_date": inicio.isoformat(), "end_date": fin.isoformat()}
        respuesta = apis.pedir_json(config.URL_ARCHIVO, params)
        archivo.write_text(json.dumps(respuesta, ensure_ascii=False), encoding="utf-8")
        print(f"  {archivo.name}: {inicio} a {fin} · {archivo.stat().st_size / 1024:,.0f} KB")
        time.sleep(config.SEGUNDOS_ENTRE_LLAMADAS)


def descargar_banco_mundial(desde: int, hasta: int) -> None:
    carpeta = DESTINO / "banco_mundial"
    carpeta.mkdir(parents=True, exist_ok=True)
    for indicador in config.INDICADORES:
        url = f"{config.URL_BANCO_MUNDIAL}/country/{';'.join(config.PAISES)}/indicator/{indicador}"
        cuerpo = apis.pedir_json(url, {"format": "json", "date": f"{desde}:{hasta}", "per_page": 2000})
        filas = cuerpo[1] or []
        (carpeta / f"{indicador}.json").write_text(json.dumps(filas, ensure_ascii=False), encoding="utf-8")
        print(f"  {indicador}: {len(filas)} registros ({sum(f['value'] is not None for f in filas)} con valor)")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--desde", type=date.fromisoformat, default=config.INICIO_CLIMA)
    p.add_argument("--hasta", type=date.fromisoformat,
                   default=date.today() - timedelta(days=config.DIAS_RETRASO_ARCHIVO + 1))
    p.add_argument("--solo", choices=["clima", "banco_mundial"])
    a = p.parse_args()
    if a.solo != "banco_mundial":
        print(f"Open-Meteo (archivo ERA5), {len(config.CIUDADES)} ciudades, {a.desde} a {a.hasta}")
        descargar_clima(a.desde, a.hasta)
    if a.solo != "clima":
        print(f"Banco Mundial, {len(config.INDICADORES)} indicadores × {len(config.PAISES)} países")
        descargar_banco_mundial(config.INICIO_BANCO_MUNDIAL, date.today().year - 1)


if __name__ == "__main__":
    main()
