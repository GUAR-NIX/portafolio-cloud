output "bucket_crudo" {
  value = google_storage_bucket.lago_crudo.url
}

output "datasets" {
  value = [for d in google_bigquery_dataset.capas : d.dataset_id]
}

output "cloud_run_job" {
  value = google_cloud_run_v2_job.pipeline.name
}
