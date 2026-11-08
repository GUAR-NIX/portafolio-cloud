# Equivalencias con el stack local:
#   MinIO (bucket ghcn)            -> Cloud Storage (lago crudo en Parquet)
#   DuckDB (staging/int/marts)     -> BigQuery (dbt-bigquery con los mismos modelos)
#   contenedor dagster-daemon      -> Cloud Run job que ejecuta el job de Dagster
#   schedule diario de Dagster     -> Cloud Scheduler que dispara el Cloud Run job

locals {
  etiquetas = { proyecto = "clima-mds", entorno = "referencia" }
  capas     = ["staging", "intermediate", "marts"]
}

resource "google_storage_bucket" "lago_crudo" {
  name                        = "${var.proyecto}-ghcn-crudo"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.etiquetas

  # los CSV comprimidos solo sirven para reprocesar: a Coldline a los 30 días
  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["csv/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

resource "google_bigquery_dataset" "capas" {
  for_each   = toset(local.capas)
  dataset_id = "clima_${each.key}"
  location   = var.region
  labels     = local.etiquetas
}

# Fuente externa: BigQuery lee el Parquet crudo directamente (equivale a read_parquet con hive_partitioning)
resource "google_bigquery_table" "ghcn_crudo" {
  dataset_id          = google_bigquery_dataset.capas["staging"].dataset_id
  table_id            = "ghcn_crudo_parquet"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.lago_crudo.name}/crudo/ghcn_diario/*"]

    hive_partitioning_options {
      mode              = "AUTO"
      source_uri_prefix = "gs://${google_storage_bucket.lago_crudo.name}/crudo/ghcn_diario/"
    }
  }
}

resource "google_service_account" "pipeline" {
  account_id   = "clima-pipeline"
  display_name = "Pipeline Dagster + dbt del clima"
}

# Mínimo privilegio: escribir objetos en su bucket y crear tablas/consultas en BigQuery
resource "google_storage_bucket_iam_member" "pipeline_objetos" {
  bucket = google_storage_bucket.lago_crudo.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_datos" {
  for_each   = google_bigquery_dataset.capas
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_consultas" {
  project = var.proyecto
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_run_v2_job" "pipeline" {
  name                = "clima-pipeline"
  location            = var.region
  deletion_protection = false
  labels              = local.etiquetas

  template {
    task_count = 1
    template {
      service_account = google_service_account.pipeline.email
      timeout         = "3600s"
      max_retries     = 1
      containers {
        image   = var.imagen
        command = ["dagster", "job", "execute", "-m", "clima_mds.definiciones", "-j", "transformaciones_dbt"]
        env {
          name  = "DBT_TARGET"
          value = "bigquery"
        }
        env {
          name  = "ANIOS"
          value = var.anios
        }
        env {
          name  = "BUCKET"
          value = google_storage_bucket.lago_crudo.name
        }
        resources {
          limits = { cpu = "2", memory = "8Gi" }
        }
      }
    }
  }
}

resource "google_service_account" "programador" {
  account_id   = "clima-programador"
  display_name = "Cloud Scheduler que dispara el pipeline"
}

resource "google_cloud_run_v2_job_iam_member" "programador_invoca" {
  name     = google_cloud_run_v2_job.pipeline.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.programador.email}"
}

resource "google_cloud_scheduler_job" "diario" {
  name      = "clima-pipeline-diario"
  region    = var.region
  schedule  = "0 7 * * *"
  time_zone = "America/Guayaquil"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.proyecto}/locations/${var.region}/jobs/${google_cloud_run_v2_job.pipeline.name}:run"
    oauth_token {
      service_account_email = google_service_account.programador.email
    }
  }
}
