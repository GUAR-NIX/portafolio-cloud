"""Clientes HTTP de Open-Meteo y del Banco Mundial.

Dos niveles de reintento:
  1. aquí, para fallos transitorios (timeouts, 429, 5xx) con espera exponencial y respeto de `Retry-After`;
  2. en Airflow (`retries` del DAG), si el error persiste después de los reintentos de este módulo.
Un 4xx distinto de 429 (parámetro mal escrito, indicador que no existe) no se reintenta: falla de inmediato.
"""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any

import requests

from ingesta import config

log = logging.getLogger(__name__)

AGENTE = "portafolio-ingesta-apis/1.0 (Airflow; uso educativo)"
TRANSITORIOS = {429, 500, 502, 503, 504}


class ErrorApi(RuntimeError):
    """La API respondió con un error que no tiene sentido reintentar, o con un cuerpo inesperado."""


def pedir_json(url: str, parametros: dict, intentos: int = 4, espera_base: float = 2.0,
               timeout: float = 30.0, sesion: requests.Session | None = None) -> Any:
    """GET que devuelve JSON, con reintentos exponenciales (2, 4, 8 s…) solo para errores transitorios."""
    s = sesion or requests.Session()
    for intento in range(1, intentos + 1):
        try:
            r = s.get(url, params=parametros, timeout=timeout, headers={"User-Agent": AGENTE})
        except (requests.ConnectionError, requests.Timeout) as e:
            if intento == intentos:
                raise
            espera = espera_base * 2 ** (intento - 1)
            log.warning("Intento %s/%s falló (%s); reintento en %.0f s", intento, intentos, e, espera)
            time.sleep(espera)
            continue
        if r.status_code in TRANSITORIOS and intento < intentos:
            espera = float(r.headers.get("Retry-After") or espera_base * 2 ** (intento - 1))
            log.warning("HTTP %s en %s; reintento %s/%s en %.0f s", r.status_code, url, intento, intentos, espera)
            time.sleep(espera)
            continue
        if r.status_code >= 400:
            raise ErrorApi(f"HTTP {r.status_code} en {r.url}: {r.text[:300]}")
        return r.json()
    raise ErrorApi(f"Sin respuesta válida de {url}")  # pragma: no cover


# ----------------------------------------------------------------------------------------------- Open-Meteo
def elegir_fuente_clima(fecha: date, hoy: date, dias_retraso: int = config.DIAS_RETRASO_ARCHIVO) -> str:
    """'archivo' (ERA5, definitivo) para fechas viejas; 'pronostico' para los últimos días.

    El archivo histórico se publica con unos días de retraso; los días recientes se piden a la API de
    pronóstico y quedan marcados como provisionales. Al re-ejecutar ese día más adelante, se reemplazan.
    """
    if fecha > hoy:
        raise ValueError(f"No se piden fechas futuras: {fecha} > {hoy}")
    return "archivo" if (hoy - fecha).days > dias_retraso else "pronostico"


def parametros_clima(fecha: date, ciudades: list[dict] | None = None) -> dict:
    ciudades = ciudades or config.CIUDADES
    return {
        "latitude": ",".join(str(c["latitud"]) for c in ciudades),
        "longitude": ",".join(str(c["longitud"]) for c in ciudades),
        "start_date": fecha.isoformat(),
        "end_date": fecha.isoformat(),
        "daily": ",".join(config.VARIABLES_CLIMA),
        "timezone": "auto",
    }


def descargar_clima(fecha: date, hoy: date, sesion: requests.Session | None = None) -> dict:
    """Una petición para las 13 ciudades de un día. Devuelve la respuesta cruda más sus metadatos."""
    fuente = elegir_fuente_clima(fecha, hoy)
    url = config.URL_ARCHIVO if fuente == "archivo" else config.URL_PRONOSTICO
    params = parametros_clima(fecha)
    respuesta = pedir_json(url, params, sesion=sesion)
    return {"fuente": fuente, "url": url, "parametros": params, "respuesta": respuesta}


# ------------------------------------------------------------------------------------------- Banco Mundial
def descargar_indicador(indicador: str, anio: int, paises: list[str] | None = None,
                        sesion: requests.Session | None = None) -> list[dict]:
    """Todas las páginas de un indicador para un año. La API responde `[metadatos, filas]`."""
    paises = paises or config.PAISES
    url = f"{config.URL_BANCO_MUNDIAL}/country/{';'.join(paises)}/indicator/{indicador}"
    filas: list[dict] = []
    pagina, paginas = 1, 1
    while pagina <= paginas:
        cuerpo = pedir_json(url, {"format": "json", "date": str(anio), "per_page": 500, "page": pagina},
                            sesion=sesion)
        if not isinstance(cuerpo, list) or not cuerpo or "message" in cuerpo[0]:
            # la API devuelve HTTP 200 con un mensaje de error dentro del JSON (p. ej., indicador inválido)
            raise ErrorApi(f"Respuesta inesperada del Banco Mundial para {indicador}: {str(cuerpo)[:300]}")
        paginas = int(cuerpo[0].get("pages") or 1)
        filas.extend(cuerpo[1] or [])
        pagina += 1
    return filas


def api_banco_mundial_disponible(sesion: requests.Session | None = None) -> bool:
    """Ping barato para el sensor: lista de fuentes de datos (no consume cuota de ningún indicador)."""
    try:
        cuerpo = pedir_json(f"{config.URL_BANCO_MUNDIAL}/sources", {"format": "json", "per_page": 1},
                            intentos=1, timeout=15, sesion=sesion)
        return isinstance(cuerpo, list) and len(cuerpo) == 2 and bool(cuerpo[1])
    except (requests.RequestException, ErrorApi, ValueError) as e:
        log.warning("API del Banco Mundial no disponible todavía: %s", e)
        return False
