output "pg_endpoint" {
  description = "PostgreSQL primaire."
  value       = module.stack.pg_endpoint
}

output "kafka_bootstrap_brokers" {
  description = "Bootstrap MSK."
  value       = module.stack.kafka_bootstrap_brokers
}

output "redis_endpoint" {
  description = "Endpoint Redis."
  value       = module.stack.redis_endpoint
}

output "ecr_repository_url" {
  description = "Dépôt ECR."
  value       = module.stack.ecr_repository_url
}

output "airflow_webserver_url" {
  description = "Interface Airflow privée."
  value       = module.stack.airflow_webserver_url
}
