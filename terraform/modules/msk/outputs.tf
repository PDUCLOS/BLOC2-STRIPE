output "bootstrap_brokers_iam" {
  description = "Chaîne bootstrap SASL/IAM (équivalent cloud de KAFKA_BROKERS)."
  value       = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
}

output "cluster_arn" {
  description = "ARN du cluster MSK."
  value       = aws_msk_cluster.this.arn
}

output "cluster_name" {
  description = "Nom du cluster (alarmes CloudWatch)."
  value       = aws_msk_cluster.this.cluster_name
}
