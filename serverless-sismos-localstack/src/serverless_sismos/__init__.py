"""Pipeline serverless local para sismos del USGS."""

from .procesamiento import procesar_geojson

__all__ = ["procesar_geojson"]

