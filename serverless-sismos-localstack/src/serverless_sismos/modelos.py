"""Modelos validados para el GeoJSON de USGS."""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PropiedadesSismo(BaseModel):
    """Campos mínimos que necesita el pipeline."""

    model_config = ConfigDict(extra="ignore")

    magnitud: float = Field(alias="mag", ge=-2.0, le=10.0)
    lugar: str = Field(alias="place", min_length=1)
    instante_ms: int = Field(alias="time", gt=0)
    actualizado_ms: int = Field(alias="updated", gt=0)
    url: str

    @property
    def instante_iso(self) -> str:
        return datetime.fromtimestamp(self.instante_ms / 1000, tz=timezone.utc).isoformat()


class GeometriaPunto(BaseModel):
    """Punto GeoJSON con orden longitud, latitud y profundidad."""

    tipo: str = Field(alias="type")
    coordenadas: tuple[float, float, float] = Field(alias="coordinates")

    @field_validator("tipo")
    @classmethod
    def exigir_punto(cls, valor: str) -> str:
        if valor != "Point":
            raise ValueError("La geometría debe ser Point")
        return valor

    @field_validator("coordenadas")
    @classmethod
    def validar_coordenadas(
        cls, valor: tuple[float, float, float]
    ) -> tuple[float, float, float]:
        longitud, latitud, profundidad = valor
        if not -180 <= longitud <= 180 or not -90 <= latitud <= 90:
            raise ValueError("Coordenadas fuera de rango")
        if profundidad < -10 or profundidad > 800:
            raise ValueError("Profundidad fuera de rango")
        return valor


class Sismo(BaseModel):
    """Evento sísmico normalizado."""

    model_config = ConfigDict(populate_by_name=True)

    identificador: str = Field(alias="id", min_length=1)
    tipo: str = Field(alias="type")
    propiedades: PropiedadesSismo = Field(alias="properties")
    geometria: GeometriaPunto = Field(alias="geometry")

    @field_validator("tipo")
    @classmethod
    def exigir_feature(cls, valor: str) -> str:
        if valor != "Feature":
            raise ValueError("El elemento debe ser Feature")
        return valor

    def como_item(self) -> dict[str, object]:
        longitud, latitud, profundidad = self.geometria.coordenadas
        return {
            "id_evento": self.identificador,
            "magnitud": str(self.propiedades.magnitud),
            "lugar": self.propiedades.lugar,
            "instante": self.propiedades.instante_iso,
            "actualizado_ms": self.propiedades.actualizado_ms,
            "longitud": str(longitud),
            "latitud": str(latitud),
            "profundidad_km": str(profundidad),
            "url": self.propiedades.url,
        }

