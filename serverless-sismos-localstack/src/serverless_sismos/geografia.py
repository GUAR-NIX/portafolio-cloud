"""Cálculos geográficos sin dependencias externas."""

from math import asin, cos, radians, sin, sqrt

CENTRO_ECUADOR = (-1.8312, -78.1834)
RADIO_TIERRA_KM = 6371.0088


def distancia_haversine_km(
    latitud_1: float, longitud_1: float, latitud_2: float, longitud_2: float
) -> float:
    """Calcula distancia de gran círculo con la fórmula de Haversine."""
    dlat = radians(latitud_2 - latitud_1)
    dlon = radians(longitud_2 - longitud_1)
    lat1 = radians(latitud_1)
    lat2 = radians(latitud_2)
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * RADIO_TIERRA_KM * asin(sqrt(a))


def distancia_a_ecuador_km(latitud: float, longitud: float) -> float:
    return distancia_haversine_km(latitud, longitud, *CENTRO_ECUADOR)

