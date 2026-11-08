# Equivalente en AWS del lakehouse local:
#   MinIO          -> S3 (bucket del lakehouse)
#   Nessie (REST)  -> AWS Glue Data Catalog (Iceberg nativo; sin ramas: ver README)
#   Trino          -> Amazon Athena (motor Trino administrado, Iceberg v2)

data "aws_caller_identity" "actual" {}

locals {
  bucket_lake       = "${var.proyecto}-${var.entorno}-${data.aws_caller_identity.actual.account_id}"
  bucket_resultados = "${var.proyecto}-${var.entorno}-athena-${data.aws_caller_identity.actual.account_id}"
  base_datos        = replace("${var.proyecto}_${var.entorno}", "-", "_")
}

# --- Almacenamiento -------------------------------------------------------------------------------------
resource "aws_s3_bucket" "lake" {
  bucket = local.bucket_lake
}

resource "aws_s3_bucket" "resultados" {
  bucket        = local.bucket_resultados
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "bloqueo" {
  for_each                = { lake = aws_s3_bucket.lake.id, resultados = aws_s3_bucket.resultados.id }
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "cifrado" {
  for_each = { lake = aws_s3_bucket.lake.id, resultados = aws_s3_bucket.resultados.id }
  bucket   = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Los dumps filtrados (landing) pasan a Infrequent Access a los 30 días; Iceberg maneja sus propios archivos
# (expire_snapshots / remove_orphan_files), así que NO se versiona el bucket ni se expiran objetos de warehouse/.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "landing-a-ia"
    status = "Enabled"
    filter {
      prefix = "landing/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
  rule {
    id     = "multipart-incompletos"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "resultados" {
  bucket = aws_s3_bucket.resultados.id
  rule {
    id     = "borrar-resultados"
    status = "Enabled"
    filter {}
    expiration {
      days = var.dias_resultados_athena
    }
  }
}

# --- Catálogo -------------------------------------------------------------------------------------------
resource "aws_glue_catalog_database" "wiki" {
  name         = local.base_datos
  description  = "Pageviews de Wikipedia (es/en) en tablas Apache Iceberg"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/warehouse/"
}

locals {
  tablas = {
    pageviews_crudo = {
      particion = [{ campo = "hora", transformacion = "day" }]
      columnas = [
        { nombre = "hora", tipo = "timestamp" }, { nombre = "archivo", tipo = "string" },
        { nombre = "linea", tipo = "bigint" }, { nombre = "dominio", tipo = "string" },
        { nombre = "titulo", tipo = "string" }, { nombre = "vistas", tipo = "string" },
        { nombre = "bytes_respuesta", tipo = "string" },
      ]
    }
    pageviews_limpio = {
      particion = [{ campo = "idioma", transformacion = "identity" }, { campo = "hora", transformacion = "hour" }]
      columnas = [
        { nombre = "hora", tipo = "timestamp" }, { nombre = "dominio", tipo = "string" },
        { nombre = "idioma", tipo = "string" }, { nombre = "plataforma", tipo = "string" },
        { nombre = "titulo", tipo = "string" }, { nombre = "vistas", tipo = "bigint" },
        { nombre = "lote", tipo = "string" },
      ]
    }
  }
}

resource "aws_glue_catalog_table" "iceberg" {
  for_each      = local.tablas
  name          = each.key
  database_name = aws_glue_catalog_database.wiki.name
  table_type    = "EXTERNAL_TABLE"

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = "2"
    }
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.lake.bucket}/warehouse/${each.key}"
    dynamic "columns" {
      for_each = each.value.columnas
      content {
        name = columns.value.nombre
        type = columns.value.tipo
      }
    }
  }

  # Glue no expresa transformaciones de partición; se definen al crear la tabla desde Athena:
  #   CREATE TABLE ... PARTITIONED BY (idioma, hour(hora)) TBLPROPERTIES ('table_type' = 'ICEBERG')
  parameters = {
    particion_iceberg = join(",", [for p in each.value.particion : "${p.transformacion}(${p.campo})"])
  }
}

# --- Motor de consultas ---------------------------------------------------------------------------------
resource "aws_athena_workgroup" "lakehouse" {
  name          = "${var.proyecto}-${var.entorno}"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.limite_bytes_por_consulta
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    result_configuration {
      output_location = "s3://${aws_s3_bucket.resultados.bucket}/resultados/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Compactación periódica equivalente a `ALTER TABLE ... EXECUTE optimize` de Trino
resource "aws_athena_named_query" "compactar_limpio" {
  name      = "compactar-pageviews-limpio"
  workgroup = aws_athena_workgroup.lakehouse.id
  database  = aws_glue_catalog_database.wiki.name
  query     = "OPTIMIZE pageviews_limpio REWRITE DATA USING BIN_PACK"
}

resource "aws_athena_named_query" "expirar_snapshots" {
  name      = "vaciar-snapshots-pageviews-limpio"
  workgroup = aws_athena_workgroup.lakehouse.id
  database  = aws_glue_catalog_database.wiki.name
  query     = "VACUUM pageviews_limpio"
}
