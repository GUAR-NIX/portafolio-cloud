"""Crea data/muestra/gharchive_muestra.json.gz: eventos CRUDOS reales de GH Archive para las pruebas y la CI.

Toma de una hora descargada hasta 120 eventos por tipo (todos los tipos aparecen, con sus payloads completos) y los
deja ordenados por created_at. Pesa ~1 MB, así que se versiona.

Los commits de un PushEvent traen correos de terceros: antes de guardar la muestra se reemplazan por un hash
(`u<12 hex>@ejemplo.invalid`; el mismo correo da el mismo hash). Las direcciones de sistema y las URL SSH
(`git@github.com`) se dejan igual. Ningún campo que use el pipeline contiene correos, así que las pruebas no cambian.

    python scripts/crear_muestra.py --hora 2025-09-16-14
    python scripts/crear_muestra.py --solo-anonimizar        # reescribe la muestra ya existente
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DESTINO = RAIZ / "data" / "muestra" / "gharchive_muestra.json.gz"

CORREO = re.compile(rb"[A-Za-z0-9._%+\-]+@(?:[A-Za-z0-9\-]+\.)+[A-Za-z]{2,}")
CORREOS_DE_SISTEMA = {b"git@github.com", b"action@github.com", b"github-actions@github.com", b"noreply@github.com"}
SAL = b"streaming-gh-muestra"


def _seudonimo(m: re.Match) -> bytes:
    correo = m.group(0)
    if correo.lower() in CORREOS_DE_SISTEMA or correo.endswith(b"@ejemplo.invalid"):
        return correo
    return b"u" + hashlib.sha256(SAL + correo.lower()).hexdigest()[:12].encode() + b"@ejemplo.invalid"


def anonimizar_linea(linea: bytes) -> bytes:
    """Reemplaza los correos de una línea JSON por un hash estable; el resto de la línea queda intacto."""
    return CORREO.sub(_seudonimo, linea)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hora", default="2025-09-16-14")
    parser.add_argument("--por-tipo", type=int, default=120)
    parser.add_argument("--solo-anonimizar", action="store_true",
                        help="reescribe la muestra existente sin volver a leer GH Archive")
    args = parser.parse_args()
    if args.solo_anonimizar:
        with gzip.open(DESTINO, "rb") as f:
            lineas = [anonimizar_linea(linea.rstrip(b"\n")) for linea in f]
        with gzip.open(DESTINO, "wb") as f:
            for linea in lineas:
                f.write(linea + b"\n")
        print(f"{len(lineas)} eventos anonimizados -> {DESTINO}")
        return
    origen = RAIZ / "data" / "raw" / "gharchive" / f"{args.hora}.json.gz"
    por_tipo: dict[str, list[tuple[str, int, bytes]]] = defaultdict(list)
    with gzip.open(origen, "rb") as f:
        for i, linea in enumerate(f):
            evento = json.loads(linea)
            lista = por_tipo[evento["type"]]
            # muestreo sistemático: uno de cada 7 para cubrir toda la hora y no solo el primer minuto
            if len(lista) < args.por_tipo and i % 7 == 0:
                lista.append((evento["created_at"], int(evento["id"]), linea.rstrip(b"\n")))
    eventos = sorted(e for lista in por_tipo.values() for e in lista)
    destino = DESTINO
    destino.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(destino, "wb") as f:
        for _, _, linea in eventos:
            f.write(anonimizar_linea(linea) + b"\n")
    print(f"{len(eventos)} eventos de {len(por_tipo)} tipos -> {destino} ({destino.stat().st_size / 1e6:.2f} MB)")


if __name__ == "__main__":
    main()
