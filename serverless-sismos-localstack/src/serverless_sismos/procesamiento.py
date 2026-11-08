"""Lógica pura compartida por las funciones Lambda y las pruebas."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import ValidationError

from .geografia import distancia_a_ecuador_km
from .modelos import Sismo


Guardar = Callable[[dict[str, object]], bool]
Alertar = Callable[[dict[str, object]], None]


def evaluar_alerta(
    sismo: Sismo, magnitud_minima: float = 5.0, distancia_maxima_km: float = 1500.0
) -> dict[str, object] | None:
    longitud, latitud, _ = sismo.geometria.coordenadas
    distancia = distancia_a_ecuador_km(latitud, longitud)
    if sismo.propiedades.magnitud < magnitud_minima or distancia > distancia_maxima_km:
        return None
    return {
        "id_evento": sismo.identificador,
        "magnitud": sismo.propiedades.magnitud,
        "lugar": sismo.propiedades.lugar,
        "distancia_ecuador_km": round(distancia, 1),
        "instante": sismo.propiedades.instante_iso,
    }


def procesar_geojson(
    documento: dict[str, Any], guardar: Guardar, alertar: Alertar
) -> dict[str, object]:
    """Valida un FeatureCollection, guarda eventos nuevos y emite alertas."""
    resumen: dict[str, object] = {
        "recibidos": 0,
        "validos": 0,
        "invalidos": 0,
        "insertados": 0,
        "duplicados": 0,
        "alertas": 0,
        "errores": [],
    }
    if documento.get("type") != "FeatureCollection" or not isinstance(
        documento.get("features"), list
    ):
        raise ValueError("Se esperaba un GeoJSON FeatureCollection")

    resumen["recibidos"] = len(documento["features"])
    for posicion, bruto in enumerate(documento["features"]):
        try:
            sismo = Sismo.model_validate(bruto)
        except ValidationError as error:
            resumen["invalidos"] += 1
            resumen["errores"].append(
                {
                    "posicion": posicion,
                    # Sin ctx: los validadores propios guardan ahí un ValueError no serializable.
                    "detalle": error.errors(include_url=False, include_context=False),
                }
            )
            continue

        resumen["validos"] += 1
        item = sismo.como_item()
        if not guardar(item):
            resumen["duplicados"] += 1
            continue
        resumen["insertados"] += 1
        alerta = evaluar_alerta(sismo)
        if alerta:
            alertar(alerta)
            resumen["alertas"] += 1
    return resumen

