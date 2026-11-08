variable "proyecto" {
  description = "ID del proyecto de GCP (marcador: no existe ningún proyecto real)"
  type        = string
  default     = "proyecto-ficticio-clima"
}

variable "region" {
  description = "Región de GCP"
  type        = string
  default     = "us-east1"
}

variable "imagen" {
  description = "Imagen del pipeline (la misma de docker/Dockerfile) en Artifact Registry"
  type        = string
  default     = "us-east1-docker.pkg.dev/proyecto-ficticio-clima/clima/clima-mds:1.0"
}

variable "anios" {
  description = "Años a procesar (particiones de Dagster)"
  type        = string
  default     = "2022,2023,2024,2025,2026"
}
