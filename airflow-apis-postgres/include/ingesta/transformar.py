"""raw (JSON tal como llegó) -> filas de staging. Funciones puras: sin red, sin base de datos, sin Airflow."""
from __future__ import annotations

from datetime import date
from typing import Any

from ingesta import config


class ErrorFormato(ValueError):
    """El JSON crudo no tiene la forma esperada: mejor fallar que cargar datos corridos."""


def filas_clima(respuesta: Any, fecha: date, fuente: str, ciudades: list[dict] | None = None) -> list[dict]:
    """Respuesta de Open-Meteo con varias ubicaciones -> una fila por ciudad y día.

    Open-Meteo devuelve una lista en el mismo orden de las coordenadas pedidas (con una sola ubicación,
    devuelve un objeto). Las coordenadas de la respuesta están ajustadas a la malla del modelo, así que no
    sirven para unir por igualdad: se une por posición y se verifica que la malla quede a menos de 0,25°.
    """
    ciudades = ciudades or config.CIUDADES
    ubicaciones = respuesta if isinstance(respuesta, list) else [respuesta]
    if len(ubicaciones) != len(ciudades):
        raise ErrorFormato(f"Se esperaban {len(ciudades)} ubicaciones y llegaron {len(ubicaciones)}")

    filas = []
    for ciudad, ub in zip(ciudades, ubicaciones, strict=True):
        if abs(ub["latitude"] - ciudad["latitud"]) > 0.25 or abs(ub["longitude"] - ciudad["longitud"]) > 0.25:
            raise ErrorFormato(f"La ubicación ({ub['latitude']}, {ub['longitude']}) no corresponde a "
                               f"{ciudad['ciudad']}: ¿cambió el orden de la respuesta?")
        diario = ub.get("daily") or {}
        tiempos = diario.get("time") or []
        if fecha.isoformat() not in tiempos:
            raise ErrorFormato(f"{ciudad['ciudad']}: la respuesta no trae el día {fecha}")
        i = tiempos.index(fecha.isoformat())
        fila = {"fecha": fecha, "ciudad": ciudad["ciudad"], "fuente": fuente,
                "latitud_malla": ub["latitude"], "longitud_malla": ub["longitude"],
                "elevacion_m": ub.get("elevation")}
        for variable, columna in config.VARIABLES_CLIMA.items():
            valores = diario.get(variable)
            if valores is None:
                raise ErrorFormato(f"Falta la variable {variable} en la respuesta")
            fila[columna] = valores[i]
        filas.append(fila)
    return filas


def filas_banco_mundial(registros: list[dict], indicador: str, anio: int) -> list[dict]:
    """Registros del Banco Mundial -> filas (país, indicador, año, valor). Los valores nulos se descartan:
    el Banco Mundial publica cada país y año aunque todavía no tenga el dato."""
    filas = []
    for r in registros:
        if r["indicator"]["id"] != indicador:
            raise ErrorFormato(f"Llegó el indicador {r['indicator']['id']} en la descarga de {indicador}")
        if int(r["date"]) != anio:
            raise ErrorFormato(f"Llegó el año {r['date']} en la descarga de {anio}")
        if r.get("value") is None:
            continue
        filas.append({
            "pais_iso3": r["countryiso3code"] or r["country"]["id"],
            "pais": r["country"]["value"],
            "indicador": indicador,
            "nombre_indicador": r["indicator"]["value"],
            "anio": anio,
            "valor": float(r["value"]),
        })
    return filas
