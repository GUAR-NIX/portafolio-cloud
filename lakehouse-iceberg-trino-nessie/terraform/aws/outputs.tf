output "bucket_lakehouse" {
  value = aws_s3_bucket.lake.bucket
}

output "base_de_datos_glue" {
  value = aws_glue_catalog_database.wiki.name
}

output "workgroup_athena" {
  value = aws_athena_workgroup.lakehouse.name
}
