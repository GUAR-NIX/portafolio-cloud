"""Prueba de carga dentro de kind: un Job de Locust golpea el Service de la API (sin port-forward).

Subcomandos:
  escenarios  1 réplica fija, N réplicas fijas y HPA (1→N→1), con muestreo del HPA cada 2 s.
  rolling     actualización 1.0.0→1.0.1 y `helm rollback` mientras corre la carga.

Todo lo que se publica en el README sale de los archivos que escribe este script en reports/.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
REPORTES = RAIZ / "reports"
HELM = str(RAIZ / "tools" / "helm.exe")
CHART = str(RAIZ / "charts" / "clima-ecuador")
IMAGEN_LOCUST = "locustio/locust:2.41.5"
OBJETIVO = "http://clima-ecuador-api:8000"
# ingress-nginx reparte por petición; el ClusterIP (kube-proxy) reparte por conexión TCP.
OBJETIVO_INGRESS = "http://ingress-nginx-controller.ingress-nginx.svc.cluster.local/api"
ARCHIVOS_LOCUST = ("stats", "stats_history", "failures")


def ejecutar(*comando: str, entrada: str | None = None, verificar: bool = True) -> str:
    resultado = subprocess.run(
        comando, input=entrada, capture_output=True, text=True, encoding="utf-8"
    )
    if verificar and resultado.returncode:
        raise RuntimeError(f"{' '.join(comando)}\n{resultado.stdout}\n{resultado.stderr}")
    return resultado.stdout


def kubectl(*argumentos: str, **opciones) -> str:
    return ejecutar("kubectl", *argumentos, **opciones)


def ahora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def hay_servicemonitor() -> bool:
    salida = kubectl("get", "crd", "servicemonitors.monitoring.coreos.com", verificar=False)
    return "servicemonitors" in salida


def helm_upgrade(**valores: str) -> float:
    """Aplica el chart con valores explícitos (sin --reuse-values) y devuelve los segundos."""
    base = {
        "ingress.enabled": "true",
        "serviceMonitor.enabled": "true" if hay_servicemonitor() else "false",
    }
    comando = [HELM, "upgrade", "clima-ecuador", CHART, "--wait", "--timeout", "5m"]
    for clave, valor in {**base, **valores}.items():
        comando += ["--set", f"{clave}={valor}"]
    inicio = time.time()
    ejecutar(*comando)
    return round(time.time() - inicio, 1)


def version_servida() -> str:
    codigo = (
        "import urllib.request;"
        f"print(urllib.request.urlopen('{OBJETIVO}/salud', timeout=5).read().decode())"
    )
    salida = kubectl("exec", "deploy/clima-ecuador-app", "--", "python", "-c", codigo)
    return json.loads(salida)["version"]


def desfase_reloj() -> float:
    """Segundos que el reloj del nodo kind va por delante del de Windows (Locust usa el del nodo)."""
    antes = time.time()
    nodo = float(ejecutar("docker", "exec", "clima-ecuador-worker", "date", "+%s.%N"))
    return nodo - (antes + time.time()) / 2


class Muestreador(threading.Thread):
    """Lee HPA y Deployment cada 2 s; sirve para medir cuánto tarda en escalar y en volver."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.filas: list[dict] = []
        self.parar = threading.Event()

    def run(self) -> None:
        while not self.parar.is_set():
            salida = kubectl(
                "get", "deploy/clima-ecuador-api", "hpa/clima-ecuador-api", "-o", "json",
                "--ignore-not-found", verificar=False,
            )
            fila = {"instante_unix": round(time.time(), 2), "listas": 0, "deseadas": None,
                    "cpu_pct_del_request": None}
            try:
                objetos = json.loads(salida).get("items", [])
            except json.JSONDecodeError:
                objetos = []
            for objeto in objetos:
                estado = objeto.get("status", {})
                if objeto["kind"] == "Deployment":
                    fila["listas"] = estado.get("readyReplicas", 0)
                else:
                    fila["deseadas"] = estado.get("desiredReplicas")
                    for metrica in estado.get("currentMetrics") or []:
                        actual = metrica.get("resource", {}).get("current", {})
                        fila["cpu_pct_del_request"] = actual.get("averageUtilization")
            self.filas.append(fila)
            self.parar.wait(2)


def publicar_locustfile() -> None:
    manifiesto = kubectl(
        "create", "configmap", "locustfile", f"--from-file={RAIZ / 'carga' / 'locustfile.py'}",
        "--dry-run=client", "-o", "yaml",
    )
    kubectl("apply", "-f", "-", entrada=manifiesto)


def manifiesto_job(nombre: str, usuarios: int, segundos: int, procesos: int, objetivo: str) -> str:
    volcado = "; ".join(
        f'echo "@@@ {archivo}"; cat /tmp/r_{archivo}.csv' for archivo in ARCHIVOS_LOCUST
    )
    script = (
        f"locust -f /carga/locustfile.py --headless -u {usuarios} -r 10 -t {segundos}s "
        f"--host {objetivo} --processes {procesos} --csv /tmp/r --only-summary; "
        f'echo "@@@ codigo $?"; {volcado}; exit 0'
    )
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": f"carga-{nombre}", "labels": {"app": "carga-locust"}},
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 900,
            "template": {
                "metadata": {"labels": {"app": "carga-locust"}},
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [{
                        "name": "locust",
                        "image": IMAGEN_LOCUST,
                        "command": ["sh", "-c", script],
                        "env": [{"name": "HOST_INGRESS",
                                 "value": "clima.local" if objetivo == OBJETIVO_INGRESS else ""}],
                        "resources": {
                            "requests": {"cpu": "500m", "memory": "256Mi"},
                            "limits": {"cpu": str(procesos), "memory": "1Gi"},
                        },
                        "volumeMounts": [{"name": "carga", "mountPath": "/carga"}],
                    }],
                    "volumes": [{"name": "carga", "configMap": {"name": "locustfile"}}],
                },
            },
        },
    }
    return json.dumps(job)


def separar_salida(logs: str) -> dict[str, str]:
    secciones: dict[str, list[str]] = {"log": []}
    actual = "log"
    for linea in logs.splitlines():
        if linea.startswith("@@@ "):
            actual = linea[4:].strip()
            secciones[actual] = []
        else:
            secciones[actual].append(linea)
    return {clave: "\n".join(lineas) + "\n" for clave, lineas in secciones.items()}


def correr_job(nombre: str, usuarios: int, segundos: int, procesos: int, durante=None,
               objetivo: str = OBJETIVO) -> dict:
    trabajo = f"carga-{nombre}"
    kubectl("delete", "job", trabajo, "--ignore-not-found", "--wait=true")
    kubectl("apply", "-f", "-", entrada=manifiesto_job(nombre, usuarios, segundos, procesos, objetivo))
    kubectl("wait", "--for=condition=Ready", "pod", "-l", f"job-name={trabajo}",
            "--timeout=300s")
    eventos = durante() if durante else {}
    kubectl("wait", "--for=condition=complete", f"job/{trabajo}", f"--timeout={segundos + 240}s")
    secciones = separar_salida(kubectl("logs", f"job/{trabajo}"))
    for archivo in ARCHIVOS_LOCUST:
        (REPORTES / f"carga_cluster_{nombre}_{archivo}.csv").write_text(
            secciones[archivo], encoding="utf-8"
        )
    estadisticas = list(csv.DictReader(io.StringIO(secciones["stats"])))
    historia = list(csv.DictReader(io.StringIO(secciones["stats_history"])))
    total = next(fila for fila in estadisticas if fila["Name"] == "Aggregated")
    activos = [f for f in historia if f["Name"] == "Aggregated" and int(f["User Count"]) > 0]
    return {
        "escenario": nombre,
        "objetivo": objetivo,
        "usuarios": usuarios,
        "duracion_segundos": segundos,
        "procesos_locust": procesos,
        "solicitudes": int(total["Request Count"]),
        "errores": int(total["Failure Count"]),
        "tasa_error_pct": round(100 * int(total["Failure Count"]) / int(total["Request Count"]), 3),
        "solicitudes_por_segundo": round(float(total["Requests/s"]), 1),
        "latencia_p50_ms": int(float(total["50%"])),
        "latencia_p95_ms": int(float(total["95%"])),
        "latencia_p99_ms": int(float(total["99%"])),
        "latencia_maxima_ms": round(float(total["Max Response Time"]), 1),
        "por_endpoint": {
            fila["Name"]: {
                "solicitudes": int(fila["Request Count"]),
                "errores": int(fila["Failure Count"]),
                "p50_ms": int(float(fila["50%"])),
                "p95_ms": int(float(fila["95%"])),
            }
            for fila in estadisticas if fila["Name"] != "Aggregated"
        },
        "advertencia_cpu_locust": "CPU usage above 90%" in secciones["log"],
        "inicio_carga_unix_nodo": int(activos[0]["Timestamp"]),
        "fin_carga_unix_nodo": int(activos[-1]["Timestamp"]),
        "eventos": eventos,
    }


def esperar_reposo(replicas: int, limite_s: int = 600) -> None:
    """Espera a que el Deployment tenga exactamente `replicas` listas y el CPU baje."""
    fin = time.time() + limite_s
    while time.time() < fin:
        estado = json.loads(kubectl("get", "deploy/clima-ecuador-api", "-o", "json"))["status"]
        if estado.get("readyReplicas") == replicas and estado.get("replicas") == replicas:
            return
        time.sleep(5)
    raise TimeoutError(f"El Deployment no volvió a {replicas} réplicas")


def esperar_hpa_en_reposo(limite_s: int = 900) -> None:
    """Exige 1 réplica deseada y lista con CPU < 20 % en tres lecturas seguidas (30 s)."""
    seguidas, fin = 0, time.time() + limite_s
    while time.time() < fin:
        objetos = json.loads(kubectl(
            "get", "deploy/clima-ecuador-api", "hpa/clima-ecuador-api", "-o", "json"))["items"]
        despliegue, hpa = (o["status"] for o in objetos)
        uso = [m["resource"]["current"].get("averageUtilization")
               for m in hpa.get("currentMetrics") or []]
        en_reposo = (despliegue.get("readyReplicas") == 1 and despliegue.get("replicas") == 1
                     and hpa.get("desiredReplicas") == 1 and uso and uso[0] is not None
                     and uso[0] < 20)
        seguidas = seguidas + 1 if en_reposo else 0
        if seguidas >= 3:
            return
        time.sleep(10)
    raise TimeoutError("El HPA no quedó en reposo con 1 réplica")


def resumir_hpa(muestras: list[dict], inicio: float, fin: float, maximo: int) -> dict:
    def primera(condicion, desde: float) -> float | None:
        for fila in muestras:
            if fila["instante_unix"] >= desde and condicion(fila):
                return round(fila["instante_unix"] - desde, 1)
        return None

    durante = [f for f in muestras if inicio <= f["instante_unix"] <= fin]
    deseadas = [f["deseadas"] for f in durante if f["deseadas"] is not None]
    cambios = [b - a for a, b in zip(deseadas, deseadas[1:]) if b != a]
    bajadas_en_carga = sum(1 for cambio in cambios if cambio < 0)
    return {
        "segundos_hasta_primera_decision_de_escalar": primera(
            lambda f: (f["deseadas"] or 0) > 1, inicio),
        "segundos_hasta_maximo_de_replicas_listas": primera(
            lambda f: f["listas"] >= maximo, inicio),
        "replicas_maximas_listas": max(f["listas"] for f in muestras),
        "cpu_pct_maximo_visto_por_hpa": max(
            (f["cpu_pct_del_request"] or 0) for f in muestras),
        "reducciones_de_replicas_durante_la_carga": bajadas_en_carga,
        "segundos_desde_fin_de_carga_hasta_1_replica": primera(
            lambda f: f["deseadas"] == 1 and f["listas"] == 1, fin),
    }


def guardar_muestras(muestras: list[dict], inicio: float, ruta: Path) -> None:
    with ruta.open("w", newline="", encoding="utf-8") as archivo:
        escritor = csv.DictWriter(
            archivo, fieldnames=["segundo", "listas", "deseadas", "cpu_pct_del_request"]
        )
        escritor.writeheader()
        for fila in muestras:
            escritor.writerow({
                "segundo": round(fila["instante_unix"] - inicio, 1),
                "listas": fila["listas"],
                "deseadas": fila["deseadas"],
                "cpu_pct_del_request": fila["cpu_pct_del_request"],
            })


def escenario_hpa(args: argparse.Namespace, desfase: float, nombre: str, objetivo: str) -> dict:
    helm_upgrade(**{"api.hpa.enabled": "true", "api.hpa.maxReplicas": str(args.max_replicas)})
    # Partir de 1 réplica ociosa: si el pod viene de otra prueba, el HPA aún ve su CPU anterior.
    esperar_hpa_en_reposo()
    muestreador = Muestreador()
    muestreador.start()
    resultado = correr_job(nombre, args.usuarios, args.segundos_hpa, args.procesos,
                           objetivo=objetivo)
    inicio = resultado["inicio_carga_unix_nodo"] - desfase
    fin = resultado["fin_carga_unix_nodo"] - desfase
    esperar_reposo(1, limite_s=900)
    time.sleep(10)
    muestreador.parar.set()
    muestreador.join()
    resultado["replicas"] = f"HPA 1→{args.max_replicas}"
    resultado["hpa"] = resumir_hpa(muestreador.filas, inicio, fin, args.max_replicas)
    guardar_muestras(muestreador.filas, inicio, REPORTES / f"carga_cluster_{nombre}_muestras.csv")
    print(json.dumps(resultado["hpa"]), flush=True)
    return resultado


def escenarios(args: argparse.Namespace) -> None:
    publicar_locustfile()
    desfase = desfase_reloj()
    ruta = REPORTES / "carga_cluster.json"
    previos = json.loads(ruta.read_text(encoding="utf-8"))["escenarios"] if ruta.exists() else []
    resultados = {e["escenario"]: {"objetivo": OBJETIVO, **e} for e in previos}
    fases = args.fases.split(",")
    if "fijos" in fases:
        for replicas in (1, args.max_replicas):
            helm_upgrade(**{"api.hpa.enabled": "false", "api.replicas": str(replicas)})
            esperar_reposo(replicas)
            time.sleep(20)
            resultado = correr_job(f"fijo-{replicas}", args.usuarios, args.segundos, args.procesos)
            resultado["replicas"] = f"{replicas} fija(s)"
            resultados[resultado["escenario"]] = resultado
            print(json.dumps({k: v for k, v in resultado.items() if k != "por_endpoint"}),
                  flush=True)
    if "hpa" in fases:
        resultados["hpa"] = escenario_hpa(args, desfase, "hpa", OBJETIVO)
    if "hpa-ingress" in fases:
        resultados["hpa-ingress"] = escenario_hpa(args, desfase, "hpa-ingress", OBJETIVO_INGRESS)

    hpa = json.loads(kubectl("get", "hpa/clima-ecuador-api", "-o", "json"))["spec"]
    orden = ["fijo-1", f"fijo-{args.max_replicas}", "hpa", "hpa-ingress"]
    lista = [resultados[n] for n in orden if n in resultados]
    resumen = {
        "fecha_utc": ahora_iso(),
        "entorno": "kind v0.30.0 (Kubernetes v1.34.0), 1 control-plane + 1 worker, Docker Desktop",
        "generador": f"Job de Locust {IMAGEN_LOCUST} dentro del clúster (sin port-forward)",
        "mezcla": "3/7 /indicadores/{ciudad}, 3/7 /clima (ciudad y fecha aleatorias), 1/7 /ciudades",
        "recursos_api": {"request_cpu": "200m", "limit_cpu": "500m"},
        "hpa": {"min": hpa["minReplicas"], "max": hpa["maxReplicas"],
                "objetivo_cpu_pct": hpa["metrics"][0]["resource"]["target"]["averageUtilization"],
                "behavior": hpa.get("behavior")},
        "desfase_reloj_nodo_s": round(desfase, 2),
        "escenarios": lista,
    }
    ruta.write_text(json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8")
    columnas = ["escenario", "replicas", "objetivo", "usuarios", "duracion_segundos", "solicitudes",
                "errores", "tasa_error_pct", "solicitudes_por_segundo", "latencia_p50_ms",
                "latencia_p95_ms", "latencia_p99_ms", "latencia_maxima_ms"]
    with (REPORTES / "carga_cluster_resumen.csv").open("w", newline="", encoding="utf-8") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=columnas, extrasaction="ignore")
        escritor.writeheader()
        escritor.writerows(lista)


def rolling(args: argparse.Namespace) -> None:
    publicar_locustfile()
    desfase = desfase_reloj()
    helm_upgrade(**{"api.hpa.enabled": "false", "api.replicas": "2"})
    esperar_reposo(2)
    revision_base = json.loads(ejecutar(HELM, "status", "clima-ecuador", "-o", "json"))["version"]

    def acciones() -> dict:
        eventos = {"version_inicial": version_servida()}
        time.sleep(20)
        eventos["upgrade_inicio_unix"] = time.time()
        eventos["upgrade_segundos"] = helm_upgrade(**{
            "api.hpa.enabled": "false", "api.replicas": "2", "api.image.tag": args.version_nueva,
        })
        eventos["version_tras_upgrade"] = version_servida()
        time.sleep(20)
        eventos["rollback_inicio_unix"] = time.time()
        inicio = time.time()
        ejecutar(HELM, "rollback", "clima-ecuador", str(revision_base), "--wait", "--timeout", "5m")
        eventos["rollback_segundos"] = round(time.time() - inicio, 1)
        eventos["version_tras_rollback"] = version_servida()
        return eventos

    resultado = correr_job("rolling", args.usuarios, args.segundos, args.procesos, acciones)
    eventos = resultado.pop("eventos")
    inicio = resultado["inicio_carga_unix_nodo"] - desfase
    for clave in ("upgrade_inicio_unix", "rollback_inicio_unix"):
        eventos[clave.replace("_unix", "_segundo")] = round(eventos.pop(clave) - inicio, 1)
    historial = ejecutar(HELM, "history", "clima-ecuador", "-o", "json")
    (REPORTES / "rolling_helm_history.json").write_text(historial, encoding="utf-8")
    resumen = {
        "fecha_utc": ahora_iso(),
        "estrategia": ("RollingUpdate maxSurge=1 maxUnavailable=0, preStop con drenaje (Connection: close) "
                       "+ 10 s, 2 réplicas, carga directa al Service (ClusterIP)"),
        "revision_restaurada": revision_base,
        **eventos,
        "carga": resultado,
    }
    (REPORTES / "rolling_update.json").write_text(
        json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(resumen, ensure_ascii=False, indent=2))
    helm_upgrade()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="comando", required=True)
    p_esc = sub.add_parser("escenarios")
    p_esc.add_argument("--usuarios", type=int, default=50)
    p_esc.add_argument("--segundos", type=int, default=120)
    p_esc.add_argument("--segundos-hpa", type=int, default=300)
    p_esc.add_argument("--max-replicas", type=int, default=4)
    p_esc.add_argument("--procesos", type=int, default=3)
    p_esc.add_argument("--fases", default="fijos,hpa,hpa-ingress",
                       help="subconjunto de fijos,hpa,hpa-ingress (los demás se conservan)")
    p_esc.set_defaults(funcion=escenarios)
    p_rol = sub.add_parser("rolling")
    p_rol.add_argument("--usuarios", type=int, default=30)
    p_rol.add_argument("--segundos", type=int, default=150)
    p_rol.add_argument("--procesos", type=int, default=2)
    p_rol.add_argument("--version-nueva", default="1.0.1")
    p_rol.set_defaults(funcion=rolling)
    args = parser.parse_args()
    REPORTES.mkdir(exist_ok=True)
    args.funcion(args)


if __name__ == "__main__":
    main()
