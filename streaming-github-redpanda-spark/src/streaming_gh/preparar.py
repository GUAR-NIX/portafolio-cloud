"""Convierte cada hora cruda de GH Archive en un archivo de eventos reducidos y ordenados por created_at.

Se hace una sola vez (parsear 718 MB de JSON por hora en cada corrida haría del productor el cuello de botella):

    docker compose run --rm productor python -m streaming_gh.preparar --procesos 4
"""

from __future__ import annotations

import argparse
import gzip
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import orjson

from streaming_gh.config import DIR_EVENTOS, DIR_RAW
from streaming_gh.reducir import reducir_evento


def preparar_hora(origen: Path, destino_dir: Path = DIR_EVENTOS) -> tuple[str, int, int, float]:
    destino = destino_dir / origen.name.replace(".json.gz", ".jsonl.gz")
    if destino.exists():
        return origen.name, -1, 0, 0.0
    t0 = time.perf_counter()
    eventos, malos = [], 0
    with gzip.open(origen, "rb") as f:
        for linea in f:
            try:
                eventos.append(reducir_evento(orjson.loads(linea)))
            except (orjson.JSONDecodeError, KeyError, TypeError):
                malos += 1
    eventos.sort(key=lambda e: (e["created_at"], int(e["id"])))
    temporal = destino.with_suffix(".parcial")
    with gzip.open(temporal, "wb", compresslevel=3) as f:
        for e in eventos:
            f.write(orjson.dumps(e) + b"\n")
    temporal.replace(destino)
    return origen.name, len(eventos), malos, time.perf_counter() - t0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--procesos", type=int, default=4)
    args = parser.parse_args()
    DIR_EVENTOS.mkdir(parents=True, exist_ok=True)
    archivos = sorted(DIR_RAW.glob("*.json.gz"))
    print(f"{len(archivos)} horas crudas en {DIR_RAW}", flush=True)
    total = 0
    with Pool(args.procesos) as pool:
        for nombre, n, malos, seg in pool.imap_unordered(preparar_hora, archivos):
            if n < 0:
                print(f"  {nombre}: ya preparado", flush=True)
                continue
            total += n
            print(f"  {nombre}: {n:,} eventos ({malos} ilegibles) en {seg:.1f} s", flush=True)
    print(f"Listo: {total:,} eventos nuevos en {DIR_EVENTOS}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
