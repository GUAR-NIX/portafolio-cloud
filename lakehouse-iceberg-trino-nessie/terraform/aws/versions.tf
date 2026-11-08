terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Referencia NO aplicada: solo se valida con `terraform fmt -check` y `terraform validate`.
provider "aws" {
  region = var.region
  default_tags {
    tags = { proyecto = var.proyecto, entorno = var.entorno, administrado_por = "terraform" }
  }
}
