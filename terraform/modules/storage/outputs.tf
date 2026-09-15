output "datalake_bucket_arn" {
  description = "ARN du bucket data lake."
  value       = aws_s3_bucket.this["datalake"].arn
}

output "datalake_bucket_name" {
  description = "Nom du bucket data lake."
  value       = aws_s3_bucket.this["datalake"].bucket
}

output "airflow_bucket_arn" {
  description = "ARN du bucket MWAA (DAGs)."
  value       = aws_s3_bucket.this["airflow"].arn
}
