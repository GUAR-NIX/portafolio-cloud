variable "region" {
  description = "Región de AWS"
  type        = string
  default     = "us-east-1"
}

variable "proyecto" {
  description = "Prefijo de los recursos"
  type        = string
  default     = "lakehouse-wiki"
}

variable "entorno" {
  description = "dev | prod"
  type        = string
  default     = "dev"
}

variable "dias_resultados_athena" {
  description = "Días que se guardan los resultados de Athena antes de borrarse"
  type        = number
  default     = 7
}

variable "limite_bytes_por_consulta" {
  description = "Corte de bytes escaneados por consulta en el workgroup (protege el costo)"
  type        = number
  default     = 10737418240 # 10 GB
}
