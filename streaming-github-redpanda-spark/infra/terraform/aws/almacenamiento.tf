# MinIO local → Amazon S3. Mismo diseño: un bucket "lakehouse" con prefijos bronce/plata/oro y _checkpoints.
resource "aws_s3_bucket" "lakehouse" {
  bucket_prefix = "${var.proyecto}-lakehouse-"
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    id     = "bronce-a-infrecuente"
    status = "Enabled"
    filter {
      prefix = "bronce/"
    }
    transition {
      days          = var.dias_retencion_bronce
      storage_class = "STANDARD_IA"
    }
  }
}

# Catálogo: las tablas Delta se registran en Glue para consultarlas con Athena (el equivalente de DuckDB local).
resource "aws_glue_catalog_database" "oro" {
  name = replace("${var.proyecto}_oro", "-", "_")
}
