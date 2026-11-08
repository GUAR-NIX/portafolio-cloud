output "bucket_lakehouse" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "msk_cluster_arn" {
  value = aws_msk_serverless_cluster.eventos.arn
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "rol_job_spark" {
  value = aws_iam_role.job_spark.arn
}
