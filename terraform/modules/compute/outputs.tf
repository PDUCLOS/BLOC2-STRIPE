output "ecr_repository_url" {
  description = "Dépôt ECR où la CI pousse l'image applicative."
  value       = aws_ecr_repository.app.repository_url
}

output "ecs_cluster_name" {
  description = "Nom du cluster ECS."
  value       = aws_ecs_cluster.this.name
}

output "service_names" {
  description = "Services ECS déployés."
  value       = [for s in aws_ecs_service.service : s.name]
}
