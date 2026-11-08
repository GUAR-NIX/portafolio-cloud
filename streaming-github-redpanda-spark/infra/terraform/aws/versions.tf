# Infraestructura de REFERENCIA: cómo se vería la plataforma en AWS. Solo se valida (fmt + validate);
# nunca se aplicó ni se aplica desde este proyecto (no hay cuenta ni credenciales).
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      proyecto = var.proyecto
      entorno  = var.entorno
    }
  }
}
