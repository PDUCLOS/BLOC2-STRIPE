output "vpc_id" {
  description = "VPC de la stack."
  value       = module.network.vpc_id
}

output "pg_endpoint" {
  description = "PostgreSQL primaire (PG_HOST)."
  value       = module.rds.endpoint
}

output "kafka_bootstrap_brokers" {
  description = "MSK bootstrap IAM (KAFKA_BROKERS)."
  value       = module.msk.bootstrap_brokers_iam
}

output "redis_endpoint" {
  description = "ElastiCache primaire (REDIS_HOST)."
  value       = module.elasticache.primary_endpoint
}

output "ecr_repository_url" {
  description = "Dépôt ECR de l'image applicative."
  value       = module.compute.ecr_repository_url
}

output "airflow_webserver_url" {
  description = "Interface MWAA (privée)."
  value       = module.airflow.webserver_url
}
