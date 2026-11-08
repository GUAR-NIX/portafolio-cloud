"""Orquesta corridas completas con Docker Compose y deja todo medido en reports/metricas/<corrida>/.

    python scripts/orquestar.py corrida --nombre principal --factor 120      # 3 días: 16–18 sep 2025
    python scripts/orquestar.py recuperacion --nombre recuperacion --matar-a 60,150
    python scripts/orquestar.py benchmark --factores 60,600,0

Cada corrida: levanta el pipeline → espera sus 5 consultas → corre el productor (y, en la de recuperación, mata el
contenedor de Spark con `docker kill` a los segundos indicados y lo vuelve a levantar) → espera a que bronce y plata
alcancen lo publicado y oro se estabilice → detiene Spark → verifica conteos (streaming_gh.verificar).

Reanudable: una corrida con orquestacion.json "terminada" se salta; si se cortó con el productor ya terminado,
solo se relanza Spark (sigue desde los checkpoints); si se cortó a mitad del productor, se borra (tema, tablas y
métricas) y se repite. Solo usa la biblioteca estándar (corre con el Python global).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
METRICAS = RAIZ / "reports" / "metricas"
sys.stdout.reconfigure(encoding="utf-8")  # con la salida redirigida, Windows usa cp1252 y falla con "→"


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def compose(*args: str, entorno: dict | None = None, check: bool = True, salida: bool = False):
    env = os.environ | (entorno or {})
    r = subprocess.run(["docker", "compose", *args], cwd=RAIZ, env=env, text=True,
                       capture_output=salida, encoding="utf-8", errors="replace")
    if check and r.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} → {r.returncode}\n{r.stderr if salida else ''}")
    return r


def leer_jsonl(ruta: Path) -> list[dict]:
    if not ruta.exists():
        return []
    filas = []
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        with contextlib.suppress(json.JSONDecodeError):  # línea a medio escribir al matar el contenedor
            filas.append(json.loads(linea))
    return filas


def totales_por_capa(dir_corrida: Path) -> dict[str, int]:
    """Filas escritas por capa; un lote repetido tras una caída se cuenta una vez (Delta también lo ignora)."""
    ultimo: dict[tuple[str, int], int] = {}
    for x in leer_jsonl(dir_corrida / "lotes.jsonl"):
        ultimo[(x["capa"], x["lote"])] = x["filas"]
    totales: dict[str, int] = {}
    for (capa, _), filas in ultimo.items():
        totales[capa] = totales.get(capa, 0) + filas
    return totales


def consultas_activas(dir_corrida: Path) -> int:
    eventos = leer_jsonl(dir_corrida / "eventos.jsonl")
    ultimo_arranque = max((i for i, e in enumerate(eventos) if e["evento"] == "arranque"), default=-1)
    return sum(1 for e in eventos[ultimo_arranque + 1:] if e["evento"] == "consulta_iniciada")


def arranques(dir_corrida: Path) -> int:
    return sum(1 for e in leer_jsonl(dir_corrida / "eventos.jsonl") if e["evento"] == "arranque")


def esperar(condicion, descripcion: str, limite_s: float, cada_s: float = 3.0) -> None:
    t0 = time.time()
    while not condicion():
        if time.time() - t0 > limite_s:
            raise TimeoutError(f"{descripcion}: más de {limite_s:.0f} s")
        time.sleep(cada_s)


def levantar_spark(entorno: dict, dir_corrida: Path) -> None:
    compose("up", "-d", "spark", entorno=entorno)
    esperar(lambda: consultas_activas(dir_corrida) >= 5, "arranque de las 5 consultas", 240)


def borrar_corrida(nombre: str) -> None:
    log(f"Borrando restos de la corrida {nombre} (tema, tablas, checkpoints, métricas)")
    compose("stop", "spark", check=False, salida=True)
    compose("exec", "-T", "redpanda", "rpk", "topic", "delete", f"gh-eventos-{nombre}", check=False, salida=True)
    compose("run", "--rm", "--entrypoint", "/bin/sh", "minio-init", "-c",
            f"mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && "
            f"mc rm -r --force local/lakehouse/{nombre} || true", check=False, salida=True)
    dir_corrida = METRICAS / nombre
    if dir_corrida.exists():
        for archivo in dir_corrida.iterdir():
            archivo.unlink()


def corriendo() -> bool:
    r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", f"{RAIZ.name.lower()}-spark-1"],
                       capture_output=True, text=True)
    return r.stdout.strip() == "true"


def corrida(nombre: str, desde: str, hasta: str, factor: float, matar_a: list[float] | None = None,
            pausa_s: float = 20, trigger_s: float = 3, limite_alcance_s: float = 3600,
            falla: str | None = None) -> dict:
    dir_corrida = METRICAS / nombre
    archivo = dir_corrida / "orquestacion.json"
    if archivo.exists() and json.loads(archivo.read_text()).get("terminada"):
        log(f"{nombre}: ya terminada, se salta")
        return json.loads(archivo.read_text())
    entorno = {"CORRIDA": nombre, "TRIGGER_S": f"{trigger_s:g}"}
    compose("up", "-d", "redpanda", "redpanda-console", "minio", "minio-init", entorno=entorno, salida=True)
    estado = {"corrida": nombre, "desde": desde, "hasta": hasta, "factor": factor, "trigger_s": trigger_s,
              "kills": [], "inicio": datetime.now(UTC).isoformat()}

    productor_listo = (dir_corrida / "productor.json").exists()
    if not productor_listo and (dir_corrida / "eventos.jsonl").exists():
        borrar_corrida(nombre)  # se cortó a mitad del productor: no se puede reanudar a medias
    dir_corrida.mkdir(parents=True, exist_ok=True)

    log(f"{nombre}: levantando el pipeline")
    levantar_spark(entorno | ({"FALLA_TRAS_ESCRIBIR": falla} if falla and not productor_listo else {}), dir_corrida)

    if not productor_listo:
        def matar():
            t0 = time.time()
            for t in matar_a or []:
                time.sleep(max(0.0, t - (time.time() - t0)))
                antes = totales_por_capa(dir_corrida)
                n_arranques = arranques(dir_corrida)
                log(f"docker kill spark (t = {t:g} s del productor)")
                subprocess.run(["docker", "kill", f"{RAIZ.name.lower()}-spark-1"], capture_output=True)
                t_kill = time.time()
                time.sleep(pausa_s)
                compose("up", "-d", "spark", entorno=entorno, salida=True)
                esperar(lambda n=n_arranques: arranques(dir_corrida) > n and consultas_activas(dir_corrida) >= 5,
                        "rearranque", 240, 1)
                estado["kills"].append({"t_productor_s": t, "ts_kill": t_kill, "ts_reanudado": time.time(),
                                        "caida_s": round(time.time() - t_kill, 1), "filas_antes": antes})
                log(f"Spark reanudado tras {time.time() - t_kill:.0f} s")

        def vigilar_falla():
            # el pipeline se mata solo tras escribir el lote indicado; se relanza sin la falla
            esperar(lambda: not corriendo(), "falla inyectada", 900, 1)
            t_caida = time.time()
            n = arranques(dir_corrida)
            log(f"Spark cayó por la falla inyectada ({falla}); relanzando en {pausa_s:g} s")
            time.sleep(pausa_s)
            compose("up", "-d", "spark", entorno=entorno | {"FALLA_TRAS_ESCRIBIR": ""}, salida=True)
            esperar(lambda: arranques(dir_corrida) > n and consultas_activas(dir_corrida) >= 5, "rearranque", 240, 1)
            estado["kills"].append({"falla": falla, "ts_kill": t_caida, "ts_reanudado": time.time(),
                                    "caida_s": round(time.time() - t_caida, 1)})

        hilo = threading.Thread(target=vigilar_falla if falla else matar, daemon=True)
        log(f"{nombre}: productor {desde} → {hasta} a {factor:g}x")
        estado["ts_productor_inicio"] = time.time()
        hilo.start()
        compose("run", "--rm", "productor", "python", "-m", "streaming_gh.productor", "--corrida", nombre,
                "--desde", desde, "--hasta", hasta, "--factor", f"{factor:g}", entorno=entorno)
        estado["ts_productor_fin"] = time.time()
        hilo.join()

    productor = json.loads((dir_corrida / "productor.json").read_text())
    log(f"{nombre}: esperando a que bronce ({productor['enviados']:,}) y plata ({productor['ids_unicos']:,}) alcancen")

    def alcanzado() -> bool:
        if consultas_activas(dir_corrida) < 5:  # p. ej. se reinició el equipo: relanzar y seguir
            compose("up", "-d", "spark", entorno=entorno, salida=True)
        t = totales_por_capa(dir_corrida)
        return t.get("bronce", 0) >= productor["enviados"] and t.get("plata", 0) >= productor["ids_unicos"]

    esperar(alcanzado, "alcance de bronce y plata", limite_alcance_s, 5)
    estado["ts_plata_alcanzada"] = time.time()
    log(f"{nombre}: plata al día; esperando que oro se estabilice")
    lineas = lambda: len(leer_jsonl(dir_corrida / "lotes.jsonl"))  # noqa: E731
    previo, quieto = -1, 0
    while quieto < 4:  # 4 × 10 s sin lotes nuevos
        time.sleep(10)
        actual = lineas()
        quieto = quieto + 1 if actual == previo else 0
        previo = actual
    compose("stop", "spark", entorno=entorno, salida=True)
    estado["fin"] = datetime.now(UTC).isoformat()
    archivo.write_text(json.dumps(estado, indent=2))

    log(f"{nombre}: verificando conteos")
    compose("run", "--rm", "tareas", "python", "-m", "streaming_gh.verificar", "--corrida", nombre,
            entorno=entorno, check=False, salida=True)
    verificacion = json.loads((dir_corrida / "verificacion.json").read_text())
    estado["exactamente_una_vez"] = verificacion["exactamente_una_vez"]
    estado["terminada"] = True
    archivo.write_text(json.dumps(estado, indent=2))
    log(f"{nombre}: terminada · exactamente una vez = {verificacion['exactamente_una_vez']}")
    return estado


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("corrida")
    c.add_argument("--nombre", required=True)
    c.add_argument("--desde", default="2025-09-16T00:00:00Z")
    c.add_argument("--hasta", default="2025-09-19T00:00:00Z")
    c.add_argument("--factor", type=float, default=120)
    c.add_argument("--trigger-s", type=float, default=3)
    r = sub.add_parser("recuperacion")
    r.add_argument("--nombre", default="recuperacion")
    r.add_argument("--desde", default="2025-09-17T00:00:00Z")
    r.add_argument("--hasta", default="2025-09-17T18:00:00Z")
    r.add_argument("--factor", type=float, default=240)
    r.add_argument("--matar-a", default="45,130,215", help="segundos desde el inicio del productor")
    r.add_argument("--repetir", action="store_true", help="borra la corrida anterior y la repite")
    r.add_argument("--pausa-s", type=float, default=20)
    fa = sub.add_parser("falla")
    fa.add_argument("--nombre", default="falla_inyectada")
    fa.add_argument("--desde", default="2025-09-18T06:00:00Z")
    fa.add_argument("--hasta", default="2025-09-18T09:00:00Z")
    fa.add_argument("--factor", type=float, default=120)
    fa.add_argument("--falla", default="plata:8", help="capa:lote tras cuya escritura muere el proceso")
    b = sub.add_parser("benchmark")
    b.add_argument("--factores", default="60,600,0")
    b.add_argument("--desde", default="2025-09-17T12:00:00Z")
    b.add_argument("--hasta", default="2025-09-17T18:00:00Z")
    args = parser.parse_args()

    if args.cmd == "corrida":
        corrida(args.nombre, args.desde, args.hasta, args.factor, trigger_s=args.trigger_s)
    elif args.cmd == "recuperacion":
        if args.repetir:
            borrar_corrida(args.nombre)
        corrida(args.nombre, args.desde, args.hasta, args.factor,
                matar_a=[float(x) for x in args.matar_a.split(",")], pausa_s=args.pausa_s)
    elif args.cmd == "falla":
        corrida(args.nombre, args.desde, args.hasta, args.factor, falla=args.falla)
    else:
        for f in args.factores.split(","):
            nombre = f"bench_{'max' if float(f) <= 0 else f'{float(f):g}x'}"
            corrida(nombre, args.desde, args.hasta, float(f))
    return 0


if __name__ == "__main__":
    sys.exit(main())
