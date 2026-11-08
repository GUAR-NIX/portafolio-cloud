"""Descarga los archivos horarios de GH Archive (https://www.gharchive.org/) a data/raw/gharchive/.

Cada hora es un .json.gz con todos los eventos públicos de GitHub de esa hora (~170 mil eventos, ~95 MB).
Es reanudable: si un archivo ya existe con el tamaño que anuncia el servidor, se salta.

    python scripts/descargar_datos.py                          # 3 días por defecto (16–18 sep 2025)
    python scripts/descargar_datos.py --inicio 2025-09-16 --dias 1 --hilos 4

Solo usa la biblioteca estándar (funciona con el Python global del equipo).
"""

from __future__ import annotations

import argparse
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

URL_BASE = "https://data.gharchive.org/{nombre}"
# Cloudflare (delante de GH Archive) responde 403 al User-Agent por defecto de urllib
CABECERAS = {"User-Agent": "portafolio-streaming-github/1.0 (+https://www.gharchive.org/)"}
RAIZ = Path(__file__).resolve().parents[1]
DESTINO = RAIZ / "data" / "raw" / "gharchive"

# Se eligió septiembre de 2025 a propósito: desde el 7-oct-2025 la API de eventos de GitHub recorta los payloads
# (sin commits ni el objeto pull_request completo), y con eso se pierde el lenguaje del repositorio.
INICIO_DEFECTO = "2025-09-16"
DIAS_DEFECTO = 3


def nombres_horarios(inicio: date, dias: int) -> list[str]:
    """Nombres de archivo de GH Archive: AAAA-MM-DD-H.json.gz (la hora va sin cero a la izquierda)."""
    nombres = []
    for d in range(dias):
        dia = inicio + timedelta(days=d)
        for h in range(24):
            nombres.append(f"{dia.isoformat()}-{h}.json.gz")
    return nombres


def tamano_remoto(url: str) -> int | None:
    peticion = urllib.request.Request(url, method="HEAD", headers=CABECERAS)
    with urllib.request.urlopen(peticion, timeout=60) as r:
        largo = r.headers.get("Content-Length")
        return int(largo) if largo else None


def descargar(nombre: str, reintentos: int = 4) -> tuple[str, int, bool]:
    url = URL_BASE.format(nombre=nombre)
    destino = DESTINO / nombre
    for intento in range(1, reintentos + 1):
        try:
            esperado = tamano_remoto(url)
            if destino.exists() and esperado and destino.stat().st_size == esperado:
                return nombre, esperado, False
            temporal = destino.with_suffix(".parcial")
            peticion = urllib.request.Request(url, headers=CABECERAS)
            with urllib.request.urlopen(peticion, timeout=120) as r, open(temporal, "wb") as f:
                while bloque := r.read(1 << 20):
                    f.write(bloque)
            if esperado and temporal.stat().st_size != esperado:
                raise OSError(f"tamaño {temporal.stat().st_size} != {esperado}")
            temporal.replace(destino)
            return nombre, destino.stat().st_size, True
        except Exception as error:  # noqa: BLE001 - red inestable: reintentar con espera creciente
            if intento == reintentos:
                raise
            print(f"  {nombre}: {error} (reintento {intento})", flush=True)
            time.sleep(5 * intento)
    raise RuntimeError("inalcanzable")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--inicio", default=INICIO_DEFECTO, help="primer día (AAAA-MM-DD)")
    parser.add_argument("--dias", type=int, default=DIAS_DEFECTO)
    parser.add_argument("--hilos", type=int, default=4)
    args = parser.parse_args()

    DESTINO.mkdir(parents=True, exist_ok=True)
    nombres = nombres_horarios(datetime.strptime(args.inicio, "%Y-%m-%d").date(), args.dias)
    print(f"{len(nombres)} archivos horarios -> {DESTINO}", flush=True)
    total, nuevos = 0, 0
    with ThreadPoolExecutor(max_workers=args.hilos) as pool:
        futuros = [pool.submit(descargar, n) for n in nombres]
        for i, futuro in enumerate(as_completed(futuros), 1):
            nombre, tamano, nuevo = futuro.result()
            total += tamano
            nuevos += nuevo
            print(f"[{i:3d}/{len(nombres)}] {nombre} {tamano / 1e6:6.1f} MB {'descargado' if nuevo else 'ya estaba'}",
                  flush=True)
    print(f"Listo: {total / 1e9:.2f} GB en {len(nombres)} archivos ({nuevos} nuevos).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
