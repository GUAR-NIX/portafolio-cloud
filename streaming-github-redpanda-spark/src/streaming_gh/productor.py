"""Productor: reproduce los eventos de GH Archive hacia Redpanda respetando sus marcas de tiempo.

El reloj virtual avanza `factor` veces más rápido que el real: con --factor 60 una hora de GitHub se publica en un
minuto; con --factor 0 se publica tan rápido como se pueda (prueba de carga). Para ejercitar el pipeline se inyectan,
de forma reproducible (semilla fija):

* duplicados: el mismo evento se reenvía unos segundos después (lo que hace un productor "al menos una vez" que
  reintenta), con la cabecera `reenvio=1`;
* tardíos: el evento sale de su lugar y se publica 3–8 min (dentro del watermark de oro) o 20–40 min (fuera de él)
  después de su created_at, con la cabecera `retraso_s`.

    python -m streaming_gh.productor --corrida principal --desde 2025-09-16T00:00:00Z --hasta 2025-09-19T00:00:00Z \
        --factor 120
"""

from __future__ import annotations

import argparse
import gzip
import heapq
import json
import random
import sys
import time
from array import array
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path

import orjson

from streaming_gh.config import DIR_EVENTOS, KAFKA_BOOTSTRAP, PARTICIONES_TEMA, Corrida


@lru_cache(maxsize=1 << 16)
def a_epoch(created_at: str) -> float:
    """'2025-09-16T14:00:00Z' -> segundos epoch (con caché: miles de eventos comparten el mismo segundo)."""
    return datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()


@dataclass(order=True)
class Mensaje:
    ts_evento: float
    id: int
    valor: bytes = field(compare=False)
    clave: bytes = field(compare=False)


def leer_archivo(ruta: Path) -> Iterator[Mensaje]:
    with gzip.open(ruta, "rb") as f:
        for linea in f:
            e = orjson.loads(linea)
            yield Mensaje(a_epoch(e["created_at"]), int(e["id"]), linea.rstrip(b"\n"),
                          str((e.get("repo") or {}).get("id", "")).encode())


def archivos_en_rango(directorio: Path, desde: datetime, hasta: datetime) -> list[Path]:
    """Horas de GH Archive que solapan [desde, hasta). Se incluye la hora anterior por los eventos que se cuelan."""
    rutas = []
    hora = desde.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    while hora < hasta:
        ruta = directorio / f"{hora:%Y-%m-%d}-{hora.hour}.jsonl.gz"
        if ruta.exists():
            rutas.append(ruta)
        hora += timedelta(hours=1)
    return rutas


def leer_eventos(rutas: list[Path], desde: float, hasta: float) -> Iterator[Mensaje]:
    """Fusiona las horas (cada archivo ya viene ordenado) y filtra por created_at en [desde, hasta)."""
    for m in heapq.merge(*(leer_archivo(r) for r in rutas)):
        if m.ts_evento >= hasta:
            break
        if m.ts_evento >= desde:
            yield m


@dataclass
class Resumen:
    corrida: str
    factor: float
    enviados: int = 0
    originales: int = 0
    duplicados: int = 0
    tardios_dentro_watermark: int = 0
    tardios_fuera_watermark: int = 0
    primer_evento: str | None = None
    ultimo_evento: str | None = None
    duracion_s: float = 0.0
    tasa_media_eps: float = 0.0
    atraso_max_s: float = 0.0  # cuánto se quedó atrás del horario el productor (0 = siguió el ritmo)
    duracion_originales_s: float = 0.0  # hasta el último evento en su hora (sin la cola de tardíos del final)
    tasa_originales_eps: float = 0.0
    ids_unicos: int = 0


def reproducir(
    eventos: Iterable[Mensaje],
    enviar: Callable[[bytes, bytes, list[tuple[str, bytes]]], None],
    factor: float,
    prob_duplicado: float = 0.0,
    prob_tardio: float = 0.0,
    semilla: int = 42,
    reloj: Callable[[], float] = time.monotonic,
    dormir: Callable[[float], None] = time.sleep,
    corrida: str = "",
    progreso: Callable[[Resumen, float], None] | None = None,
) -> Resumen:
    """Núcleo del productor, sin Kafka (se prueba con un `enviar` falso y un reloj simulado)."""
    rnd = random.Random(semilla)
    r = Resumen(corrida=corrida, factor=factor)
    pendientes: list[tuple[float, int, Mensaje, list[tuple[str, bytes]]]] = []  # (ts_liberacion, orden, ...)
    orden = 0
    t0_evento: float | None = None
    t0_real = reloj()
    ultimo_progreso = t0_real
    # ids en un arreglo de int64 (8 B por evento): un set de Python con 11 M de enteros ocupa ~900 MB y el
    # contenedor del productor (768 MB) moría por memoria al tercer día de la corrida principal
    ids = array("q")

    def esperar_hasta(ts_evento: float) -> None:
        if factor <= 0 or t0_evento is None:
            return
        objetivo = t0_real + (ts_evento - t0_evento) / factor
        falta = objetivo - reloj()
        if falta > 0.002:
            dormir(falta)
        else:
            r.atraso_max_s = max(r.atraso_max_s, -falta)

    def liberar_pendientes(hasta_ts: float) -> None:
        while pendientes and pendientes[0][0] <= hasta_ts:
            ts_lib, _, m, cabeceras = heapq.heappop(pendientes)
            esperar_hasta(ts_lib)
            enviar(m.valor, m.clave, cabeceras)
            r.enviados += 1

    for m in eventos:
        if t0_evento is None:
            t0_evento = m.ts_evento
            r.primer_evento = datetime.fromtimestamp(m.ts_evento, UTC).isoformat()
        r.ultimo_evento = datetime.fromtimestamp(m.ts_evento, UTC).isoformat()
        ids.append(m.id)
        r.originales += 1
        liberar_pendientes(m.ts_evento)

        sorteo = rnd.random()
        if sorteo < prob_tardio:
            if rnd.random() < 0.5:
                retraso = rnd.uniform(3 * 60, 8 * 60)
                r.tardios_dentro_watermark += 1
            else:
                retraso = rnd.uniform(20 * 60, 40 * 60)
                r.tardios_fuera_watermark += 1
            orden += 1
            heapq.heappush(pendientes, (m.ts_evento + retraso, orden, m, [("retraso_s", str(int(retraso)).encode())]))
            continue

        esperar_hasta(m.ts_evento)
        enviar(m.valor, m.clave, [])
        r.enviados += 1
        if sorteo < prob_tardio + prob_duplicado:
            orden += 1
            r.duplicados += 1
            heapq.heappush(pendientes, (m.ts_evento + rnd.uniform(1, 30), orden, m, [("reenvio", b"1")]))

        if progreso and reloj() - ultimo_progreso >= 10:
            ultimo_progreso = reloj()
            progreso(r, ultimo_progreso - t0_real)

    r.duracion_originales_s = reloj() - t0_real
    r.tasa_originales_eps = r.enviados / r.duracion_originales_s if r.duracion_originales_s > 0 else 0.0
    liberar_pendientes(float("inf"))
    r.duracion_s = reloj() - t0_real
    r.tasa_media_eps = r.enviados / r.duracion_s if r.duracion_s > 0 else 0.0
    import numpy as np

    r.ids_unicos = int(np.unique(np.frombuffer(ids, dtype=np.int64)).size) if ids else 0
    return r


# ---------------------------------------------------------------------------------------------------------- Kafka

def crear_tema(tema: str, particiones: int = PARTICIONES_TEMA) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    if tema in admin.list_topics(timeout=30).topics:
        return
    futuros = admin.create_topics([NewTopic(tema, num_partitions=particiones, replication_factor=1)])
    futuros[tema].result()
    print(f"Tema {tema} creado con {particiones} particiones", flush=True)


def productor_kafka():
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,  # sin duplicados por reintentos de red: los únicos duplicados son los inyectados
        "compression.type": "lz4",
        "linger.ms": 20,
        "batch.size": 1_000_000,
        "queue.buffering.max.messages": 500_000,
    })


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--corrida", required=True)
    parser.add_argument("--desde", default="2025-09-16T00:00:00Z")
    parser.add_argument("--hasta", default="2025-09-19T00:00:00Z")
    parser.add_argument("--factor", type=float, default=120.0, help="aceleración del reloj; 0 = lo más rápido posible")
    parser.add_argument("--prob-duplicado", type=float, default=0.01)
    parser.add_argument("--prob-tardio", type=float, default=0.002)
    parser.add_argument("--semilla", type=int, default=42)
    args = parser.parse_args()

    corrida = Corrida(args.corrida)
    desde = datetime.fromisoformat(args.desde.replace("Z", "+00:00"))
    hasta = datetime.fromisoformat(args.hasta.replace("Z", "+00:00"))
    rutas = archivos_en_rango(DIR_EVENTOS, desde, hasta)
    if not rutas:
        print(f"No hay eventos preparados en {DIR_EVENTOS} (correr streaming_gh.preparar)", file=sys.stderr)
        return 1
    crear_tema(corrida.tema)
    productor = productor_kafka()
    errores = []

    def al_entregar(err, _msg):
        if err is not None:
            errores.append(str(err))

    def enviar(valor: bytes, clave: bytes, cabeceras: list[tuple[str, bytes]]) -> None:
        while True:
            try:
                productor.produce(corrida.tema, value=valor, key=clave, headers=cabeceras or None,
                                  on_delivery=al_entregar)
                break
            except BufferError:
                productor.poll(0.05)
        productor.poll(0)

    def progreso(r: Resumen, seg: float) -> None:
        print(f"  {seg:7.0f} s · {r.enviados:,} enviados · {r.enviados / seg:,.0f} ev/s · reloj virtual "
              f"{r.ultimo_evento} · atraso máx {r.atraso_max_s:.1f} s", flush=True)

    print(f"Reproduciendo {desde:%Y-%m-%d %H:%M} → {hasta:%Y-%m-%d %H:%M} UTC ({len(rutas)} archivos) a "
          f"{'máxima velocidad' if args.factor <= 0 else f'{args.factor:g}x'} hacia {corrida.tema}", flush=True)
    resumen = reproducir(
        leer_eventos(rutas, desde.timestamp(), hasta.timestamp()), enviar, args.factor,
        args.prob_duplicado, args.prob_tardio, args.semilla, corrida=corrida.nombre, progreso=progreso,
    )
    pendientes = productor.flush(120)
    resumen.duracion_s = round(resumen.duracion_s, 2)
    resumen.tasa_media_eps = round(resumen.tasa_media_eps, 1)
    resumen.duracion_originales_s = round(resumen.duracion_originales_s, 2)
    resumen.tasa_originales_eps = round(resumen.tasa_originales_eps, 1)
    datos = asdict(resumen) | {"errores_entrega": len(errores), "sin_confirmar": pendientes,
                               "desde": args.desde, "hasta": args.hasta, "semilla": args.semilla,
                               "prob_duplicado": args.prob_duplicado, "prob_tardio": args.prob_tardio,
                               "terminado_en": datetime.now(UTC).isoformat()}
    corrida.dir_metricas.mkdir(parents=True, exist_ok=True)
    (corrida.dir_metricas / "productor.json").write_text(json.dumps(datos, indent=2, ensure_ascii=False))
    print(json.dumps(datos, indent=2, ensure_ascii=False))
    return 0 if not errores and pendientes == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
