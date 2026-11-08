"""Sube un feed a S3 local, una vez o cada N minutos.

Sin opciones sube el archivo que ya está en disco (así se reanuda sin descargar otra vez). Con `--cada N` descarga el
feed de USGS cada N minutos, lo guarda de forma atómica y lo sube con una clave nueva; un fallo de red se registra y
el ciclo sigue con la siguiente vuelta.

    python scripts/productor.py                              # sube data/raw/usgs_hora.geojson una vez
    python scripts/productor.py --cada 5                     # descarga all_hour cada 5 min hasta Ctrl+C
    python scripts/productor.py --cada 5 --iteraciones 3     # tres vueltas y termina
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))  # para importar descargar_datos.py al correr `python scripts/productor.py`

from descargar_datos import FEEDS  # noqa: E402


def clave_s3(ahora: datetime) -> str:
    return f"entrada/usgs-{ahora.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.geojson"


def producir(
    archivo: Path,
    subir: Callable[[Path, str], None],
    *,
    cada_minutos: float | None = None,
    descargar: Callable[[Path], object] | None = None,
    iteraciones: int | None = None,
    dormir: Callable[[float], None] = time.sleep,
    ahora: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    registrar: Callable[[str], None] = print,
) -> list[str]:
    """Sube `archivo` y devuelve las claves subidas.

    Sin `cada_minutos` hace una sola subida del archivo existente. Con `cada_minutos` repite: descarga (si hay
    `descargar`), sube y duerme `cada_minutos` entre vueltas; se detiene tras `iteraciones` vueltas o con Ctrl+C.
    Si la descarga falla no se sube nada en esa vuelta (no se reenvía un archivo viejo como si fuera nuevo).
    """
    subidas: list[str] = []
    vuelta = 0
    try:
        while True:
            vuelta += 1
            if cada_minutos is None:
                clave = clave_s3(ahora())
                subir(archivo, clave)
                subidas.append(clave)
                break
            try:
                if descargar is not None:
                    descargar(archivo)
            except Exception as error:  # noqa: BLE001 - una vuelta fallida no debe matar al productor
                registrar(f"vuelta {vuelta}: descarga fallida ({error}); se reintenta en {cada_minutos:g} min")
            else:
                clave = clave_s3(ahora())
                subir(archivo, clave)
                subidas.append(clave)
                registrar(f"vuelta {vuelta}: subido {clave}")
            if iteraciones is not None and vuelta >= iteraciones:
                break
            dormir(cada_minutos * 60)
    except KeyboardInterrupt:
        registrar("detenido por el usuario")
    return subidas


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--archivo", type=Path, default=Path("data/raw/usgs_hora.geojson"))
    parser.add_argument("--cubo", default="sismos-entradas-local")
    parser.add_argument("--endpoint", default="http://localhost:4566")
    parser.add_argument("--cada", type=float, metavar="N", help="descarga y sube el feed cada N minutos")
    parser.add_argument("--feed", choices=sorted(FEEDS), default="hora", help="feed de USGS con --cada")
    parser.add_argument("--iteraciones", type=int, help="con --cada: número de vueltas (por defecto, sin fin)")
    args = parser.parse_args()
    if args.cada is not None and args.cada <= 0:
        parser.error("--cada debe ser mayor que 0")

    import boto3

    cliente = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    def subir(ruta: Path, clave: str) -> None:
        cliente.upload_file(str(ruta), args.cubo, clave)
        print(f"s3://{args.cubo}/{clave}", flush=True)

    descargar = None
    if args.cada is not None:
        from descargar_datos import descargar as descargar_feed

        def descargar(destino: Path) -> object:
            return descargar_feed(FEEDS[args.feed], destino)

    producir(
        args.archivo,
        subir,
        cada_minutos=args.cada,
        descargar=descargar,
        iteraciones=args.iteraciones,
        registrar=lambda texto: print(texto, flush=True),
    )


if __name__ == "__main__":
    main()
