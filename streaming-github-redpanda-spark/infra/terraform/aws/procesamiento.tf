# Spark en contenedor local → EMR Serverless (Spark 3.5 en emr-7.x, Delta Lake incluido).
# El job es el mismo pipeline.py con --conf de Delta y el bootstrap de MSK; los checkpoints van a S3.
resource "aws_security_group" "emr" {
  name_prefix = "${var.proyecto}-emr-"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_emrserverless_application" "spark" {
  name          = "${var.proyecto}-spark"
  release_label = "emr-7.5.0"
  type          = "spark"

  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  network_configuration {
    subnet_ids         = var.subredes_privadas
    security_group_ids = [aws_security_group.emr.id]
  }
}

data "aws_iam_policy_document" "confianza_emr" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "job_spark" {
  name_prefix        = "${var.proyecto}-job-"
  assume_role_policy = data.aws_iam_policy_document.confianza_emr.json
}

data "aws_iam_policy_document" "job_spark" {
  statement {
    sid       = "LakehouseLecturaEscritura"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse.arn, "${aws_s3_bucket.lakehouse.arn}/*"]
  }
  statement {
    sid = "MskConsumir"
    actions = [
      "kafka-cluster:Connect", "kafka-cluster:DescribeTopic", "kafka-cluster:ReadData",
      "kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup",
    ]
    resources = [
      aws_msk_serverless_cluster.eventos.arn,
      "arn:aws:kafka:${var.region}:*:topic/${aws_msk_serverless_cluster.eventos.cluster_name}/*",
      "arn:aws:kafka:${var.region}:*:group/${aws_msk_serverless_cluster.eventos.cluster_name}/*",
    ]
  }
  statement {
    sid       = "CatalogoGlue"
    actions   = ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable", "glue:UpdateTable"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "job_spark" {
  role   = aws_iam_role.job_spark.id
  policy = data.aws_iam_policy_document.job_spark.json
}
