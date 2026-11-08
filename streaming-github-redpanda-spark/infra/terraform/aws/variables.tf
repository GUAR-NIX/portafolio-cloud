variable "region" {
  description = "Región de AWS"
  type        = string
  default     = "us-east-1"
}

variable "proyecto" {
  type    = string
  default = "streaming-github"
}

variable "entorno" {
  type    = string
  default = "dev"
}

variable "vpc_id" {
  description = "VPC existente donde viven MSK y EMR Serverless"
  type        = string
}

variable "subredes_privadas" {
  description = "Al menos dos subredes privadas en zonas distintas"
  type        = list(string)
}

variable "dias_retencion_bronce" {
  description = "Días que se conservan los objetos de bronce antes de pasar a almacenamiento infrecuente"
  type        = number
  default     = 30
}
