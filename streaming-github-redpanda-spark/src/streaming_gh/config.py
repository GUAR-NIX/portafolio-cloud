"""Configuración central. Todo se puede sobreescribir con variables de entorno (docker-compose.yml las fija)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

RAIZ = Path(os.environ.get("PROYECTO_RAIZ", Path(__file__).resolve().parents[2]))
DIR_RAW = RAIZ / "data" / "raw" / "gharchive"
DIR_EVENTOS = RAIZ / "data" / "interim" / "eventos"  # eventos reducidos y ordenados por created_at (uno por hora)
DIR_MUESTRA = RAIZ / "data" / "muestra"
DIR_METRICAS = RAIZ / "reports" / "metricas"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
PARTICIONES_TEMA = int(os.environ.get("PARTICIONES_TEMA", "6"))

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")  # credenciales de MinIO local, no de una nube
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
BUCKET = os.environ.get("BUCKET", "lakehouse")

# Ventanas y watermarks (tiempo de evento = created_at de GitHub)
WATERMARK_PLATA = os.environ.get("WATERMARK_PLATA", "2 hours")  # cuánto se recuerda un id para deduplicar
WATERMARK_ORO = os.environ.get("WATERMARK_ORO", "10 minutes")  # cuánto se espera a los tardíos antes de cerrar
VENTANA_ACTIVIDAD = "5 minutes"
VENTANA_HORA = "1 hour"


@dataclass(frozen=True)
class Corrida:
    """Una corrida aísla tema de Kafka, tablas y checkpoints: principal, recuperación y cada benchmark."""

    nombre: str
    raiz: str | None = None  # para pruebas: una carpeta local en lugar de MinIO
    dir_metricas_override: Path | None = None

    @property
    def tema(self) -> str:
        return f"gh-eventos-{self.nombre}"

    @property
    def base(self) -> str:
        return self.raiz or f"s3a://{BUCKET}/{self.nombre}"

    def tabla(self, capa: str, nombre: str) -> str:
        return f"{self.base}/{capa}/{nombre}"

    def checkpoint(self, consulta: str) -> str:
        return f"{self.base}/_checkpoints/{consulta}"

    @property
    def dir_metricas(self) -> Path:
        return self.dir_metricas_override or DIR_METRICAS / self.nombre


TABLAS = {
    "bronce": ["eventos"],
    "plata": ["eventos"],
    "oro": ["actividad_5min", "repos_hora", "lenguajes_hora", "picos_actividad", "picos_repos"],
}
