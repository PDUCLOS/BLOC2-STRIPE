output "primary_endpoint" {
  description = "Endpoint primaire (équivalent cloud de REDIS_HOST)."
  value       = aws_elasticache_replication_group.this.primary_endpoint_address
}

output "replication_group_id" {
  description = "Identifiant du groupe de réplication (alarmes CloudWatch)."
  value       = aws_elasticache_replication_group.this.id
}
