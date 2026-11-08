provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    dynamodb = var.endpoint_local
    iam      = var.endpoint_local
    lambda   = var.endpoint_local
    s3       = var.endpoint_local
    sns      = var.endpoint_local
    sqs      = var.endpoint_local
    sts      = var.endpoint_local
  }
}

variable "endpoint_local" {
  type        = string
  description = "Endpoint exclusivo del emulador LocalStack"
  default     = "http://localhost:4566"

  validation {
    condition     = can(regex("^https?://(localhost|127\\.0\\.0\\.1)(:|/)", var.endpoint_local))
    error_message = "Por seguridad solo se permite LocalStack en localhost."
  }
}

resource "aws_s3_bucket" "entradas" {
  bucket        = "sismos-entradas-local"
  force_destroy = true
}

resource "aws_dynamodb_table" "sismos" {
  name         = "sismos"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id_evento"
  attribute {
    name = "id_evento"
    type = "S"
  }
  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"
}

resource "aws_sns_topic" "alertas" {
  name = "alertas-sismos"
}

# Consumidor de auditoría: recibe cada alerta publicada en SNS. Permite medir la
# latencia S3 -> alerta sin depender de correo o SMS.
resource "aws_sqs_queue" "buzon_alertas" {
  name                      = "buzon-alertas-sismos"
  message_retention_seconds = 3600
}

resource "aws_sqs_queue_policy" "buzon_alertas" {
  queue_url = aws_sqs_queue.buzon_alertas.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.buzon_alertas.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_sns_topic.alertas.arn } }
    }]
  })
}

resource "aws_sns_topic_subscription" "buzon_alertas" {
  topic_arn            = aws_sns_topic.alertas.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.buzon_alertas.arn
  raw_message_delivery = true
}

resource "aws_iam_role" "lambda" {
  name = "lambda-sismos-local"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name = "acceso-pipeline-local"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "dynamodb:PutItem", "sns:Publish", "logs:*"]
      Resource = "*"
    }]
  })
}

data "archive_file" "lambda_ingesta" {
  type        = "zip"
  source_dir  = "${path.module}/build/lambda"
  output_path = "${path.module}/lambda_ingesta.zip"
}

resource "aws_lambda_function" "ingesta" {
  function_name    = "ingestar-sismos"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "app.manejador"
  filename         = data.archive_file.lambda_ingesta.output_path
  source_code_hash = data.archive_file.lambda_ingesta.output_base64sha256
  # Un feed semanal (~2.000 eventos) tarda varios segundos en el emulador; 120 s evita
  # que un timeout dispare el reintento asíncrono de S3.
  timeout     = 120
  memory_size = 256
  environment {
    variables = {
      TABLA_SISMOS     = aws_dynamodb_table.sismos.name
      ARN_TEMA_ALERTAS = aws_sns_topic.alertas.arn
      PYTHONPATH       = "/var/task"
    }
  }
}

resource "aws_lambda_permission" "desde_s3" {
  statement_id  = "PermitirS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingesta.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.entradas.arn
}

resource "aws_s3_bucket_notification" "evento" {
  bucket = aws_s3_bucket.entradas.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.ingesta.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "entrada/"
    filter_suffix       = ".geojson"
  }
  depends_on = [aws_lambda_permission.desde_s3]
}

output "cubo" { value = aws_s3_bucket.entradas.bucket }
output "tabla" { value = aws_dynamodb_table.sismos.name }
output "tema_alertas" { value = aws_sns_topic.alertas.arn }
output "buzon_alertas" { value = aws_sqs_queue.buzon_alertas.id }

