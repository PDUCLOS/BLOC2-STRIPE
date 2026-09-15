output "endpoint" {
  description = "Endpoint du primaire (écritures : producer, write-back du scorer)."
  value       = aws_db_instance.primary.address
}

output "reader_endpoints" {
  description = "Endpoints des réplicas (dashboard, analytics_reader)."
  value       = aws_db_instance.read_replica[*].address
}

output "master_user_secret_arn" {
  description = "Secret Secrets Manager du mot de passe maître (géré par RDS)."
  value       = aws_db_instance.primary.master_user_secret[0].secret_arn
}

output "instance_id" {
  description = "Identifiant de l'instance primaire (alarmes CloudWatch)."
  value       = aws_db_instance.primary.identifier
}
