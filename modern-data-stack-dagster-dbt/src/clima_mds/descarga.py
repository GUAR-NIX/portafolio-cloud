"""Descarga reanudable de archivos de NOAA GHCN-Daily (AWS Open Data, acceso anónimo por HTTPS).

La PC se ha apagado varias veces: se descarga a un archivo .part y, si existe, se pide solo lo que falta
(cabecera Range con If-Range=ETag). Si el archivo remoto cambió (el año en curso se actualiza cada día),
se vuelve a bajar entero.
"""

from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

TAMANO_BLOQUE = 1024 * 1024


@dataclass
class ResultadoDescarga:
    ruta: Path
    bytes_totales: int
    bytes_descargados: int
    segundos: float
    etag: str
    reutilizado: bool


def cabeceras_remotas(url: str, abrir=urllib.request.urlopen) -> tuple[int, str]:
    """Tamaño y ETag del objeto remoto (petición HEAD)."""
    with abrir(urllib.request.Request(url, method="HEAD"), timeout=60) as resp:
        return int(resp.headers["Content-Length"]), resp.headers.get("ETag", "").strip('"')


def descargar_reanudable(url: str, destino: Path, abrir=urllib.request.urlopen) -> ResultadoDescarga:
    """Descarga `url` a `destino`; reutiliza el archivo si ya está completo y con el mismo ETag."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = destino.with_name(destino.name + ".part")
    sidecar = destino.with_name(destino.name + ".json")
    tamano, etag = cabeceras_remotas(url, abrir)
    inicio = time.perf_counter()

    previo = json.loads(sidecar.read_text()) if sidecar.exists() else {}
    if destino.exists() and destino.stat().st_size == tamano and previo.get("etag") == etag:
        return ResultadoDescarga(destino, tamano, 0, 0.0, etag, True)

    # Reanudar solo si el .part corresponde a la misma versión del objeto.
    ya = parcial.stat().st_size if parcial.exists() and previo.get("etag_parcial") == etag else 0
    sidecar.write_text(json.dumps({"etag_parcial": etag, "url": url}))
    peticion = urllib.request.Request(url)
    if ya:
        peticion.add_header("Range", f"bytes={ya}-")
        peticion.add_header("If-Range", f'"{etag}"')
    descargados = 0
    with abrir(peticion, timeout=120) as resp:
        if ya and resp.status != 206:  # el servidor ignoró Range: empezar de cero
            ya = 0
        with open(parcial, "ab" if ya else "wb") as f:
            while bloque := resp.read(TAMANO_BLOQUE):
                f.write(bloque)
                descargados += len(bloque)
    if parcial.stat().st_size != tamano:
        raise OSError(f"Descarga incompleta de {url}: {parcial.stat().st_size} de {tamano} bytes")
    parcial.replace(destino)
    sidecar.write_text(json.dumps({"etag": etag, "url": url, "bytes": tamano}))
    return ResultadoDescarga(destino, tamano, descargados, time.perf_counter() - inicio, etag, False)
