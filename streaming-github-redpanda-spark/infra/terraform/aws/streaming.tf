# Redpanda local → Amazon MSK Serverless (API de Kafka, autenticación IAM). Alternativa: Kinesis Data Streams.
resource "aws_security_group" "msk" {
  name_prefix = "${var.proyecto}-msk-"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Kafka con IAM desde EMR Serverless"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.emr.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_serverless_cluster" "eventos" {
  cluster_name = "${var.proyecto}-eventos"

  vpc_config {
    subnet_ids         = var.subredes_privadas
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}
