"""Descarga reanudable y respetuosa de los dumps horarios de pageviews de Wikimedia.

Cada archivo horario (~55-65 MB comprimido, ~7 M de líneas) se lee en streaming y solo se guardan las líneas de
Wikipedia en español e inglés (escritorio y móvil) con al menos `MIN_VISTAS` vistas en la hora. La cola larga de
1-4 vistas es ~86 % de las filas pero una fracción menor de las vistas (el manifiesto registra cuánta), y no cambia
ningún artículo en tendencia. En disco queda ~3 MB por hora y nunca se guarda el archivo completo.

Reanudable por archivo: lo filtrado se escribe en `*.part` y se renombra al terminar; un manifiesto JSON por hora
registra bytes leídos, líneas y sha256. Si el archivo final y su manifiesto existen, la hora se salta.
Respeto al servidor: una sola conexión a la vez, pausa entre archivos, reintentos con espera creciente y un
User-Agent que identifica el proyecto (política de User-Agent de Wikimedia).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

URL_BASE = "https://dumps.wikimedia.org/other/pageviews"
USER_AGENT = "lakehouse-iceberg-portafolio/1.0 (proyecto educativo; descarga secuencial con pausas)"
PROYECTOS = ("en", "en.m", "es", "es.m")
MIN_VISTAS = 5


@dataclass
class ResultadoHora:
    hora: str
    archivo: str
    bytes_origen: int
    lineas_origen: int
    lineas_guardadas: int
    vistas_proyectos: int  # todas las vistas de en/es en la hora (antes del umbral)
    vistas_guardadas: int
    bytes_guardados: int
    sha256: str
    segundos: float
    reutilizado: bool = False


def url_hora(h: datetime) -> str:
    return f"{URL_BASE}/{h:%Y}/{h:%Y-%m}/pageviews-{h:%Y%m%d-%H}0000.gz"


def horas_de_dias(dias: list[str]) -> list[datetime]:
    """Las 24 horas UTC de cada día AAAA-MM-DD, en orden."""
    horas = []
    for d in dias:
        inicio = datetime.strptime(d, "%Y-%m-%d")
        horas += [inicio + timedelta(hours=i) for i in range(24)]
    return horas


def linea_de_proyecto(linea: bytes, proyectos: tuple[str, ...] = PROYECTOS) -> bool:
    """True si la línea `dominio título vistas tamaño` es de uno de los proyectos pedidos."""
    espacio = linea.find(b" ")
    return espacio > 0 and linea[:espacio].decode("ascii", "replace") in proyectos


def vistas_de_linea(linea: bytes) -> int:
    """Tercer campo como entero; 0 si la línea está mal formada (la limpieza la contará como inválida)."""
    partes = linea.split(b" ")
    return int(partes[2]) if len(partes) == 4 and partes[2].isdigit() else 0


def _sha256(ruta: Path) -> str:
    h = hashlib.sha256()
    with ruta.open("rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return h.hexdigest()


def descargar_hora(h: datetime, destino: Path, reintentos: int = 4, timeout: int = 120) -> ResultadoHora:
    destino.mkdir(parents=True, exist_ok=True)
    nombre = f"pageviews-{h:%Y%m%d-%H}0000-enes.gz"
    final, parcial, manifiesto = destino / nombre, destino / (nombre + ".part"), destino / (nombre + ".json")
    if final.exists() and manifiesto.exists():
        datos = json.loads(manifiesto.read_text(encoding="utf-8"))
        if datos.get("sha256") == _sha256(final):
            return ResultadoHora(**{**datos, "reutilizado": True})

    espera = 5.0
    for intento in range(1, reintentos + 1):
        t0 = time.perf_counter()
        try:
            req = urllib.request.Request(url_hora(h), headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                contador = _LectorContado(resp)
                lineas_origen = guardadas = vistas_proy = vistas_guard = 0
                with gzip.GzipFile(fileobj=contador) as entrada, gzip.open(parcial, "wb", compresslevel=6) as salida:
                    for linea in entrada:
                        lineas_origen += 1
                        if linea_de_proyecto(linea):
                            v = vistas_de_linea(linea)
                            vistas_proy += v
                            # las líneas mal formadas pasan para que la capa limpia las cuente y descarte
                            if v >= MIN_VISTAS or v == 0:
                                salida.write(linea)
                                guardadas += 1
                                vistas_guard += v
            parcial.replace(final)
            res = ResultadoHora(hora=f"{h:%Y-%m-%dT%H:00}", archivo=nombre, bytes_origen=contador.n,
                                lineas_origen=lineas_origen, lineas_guardadas=guardadas,
                                vistas_proyectos=vistas_proy, vistas_guardadas=vistas_guard,
                                bytes_guardados=final.stat().st_size, sha256=_sha256(final),
                                segundos=round(time.perf_counter() - t0, 1))
            datos = asdict(res)
            datos.pop("reutilizado")
            manifiesto.write_text(json.dumps(datos, indent=1), encoding="utf-8")
            return res
        except (OSError, EOFError) as e:  # urllib.error.URLError hereda de OSError
            parcial.unlink(missing_ok=True)
            if intento == reintentos:
                raise
            print(f"  {h:%Y-%m-%d %H}h intento {intento} falló ({e}); reintento en {espera:.0f} s", flush=True)
            time.sleep(espera)
            espera *= 2
    raise RuntimeError("inalcanzable")


class _LectorContado:
    """Envuelve la respuesta HTTP para contar los bytes comprimidos leídos."""

    def __init__(self, f):
        self.f, self.n = f, 0

    def read(self, n=-1):
        b = self.f.read(n)
        self.n += len(b)
        return b
