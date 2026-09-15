output "kms_key_arn" {
  description = "ARN de la CMK de la stack."
  value       = aws_kms_key.this.arn
}

output "app_security_group_id" {
  description = "Security group des tâches applicatives."
  value       = aws_security_group.app.id
}

output "data_security_group_ids" {
  description = "Security groups data par service (rds, msk, redis, mongo)."
  value       = { for k, sg in aws_security_group.data : k => sg.id }
}

output "ecs_execution_role_arn" {
  description = "Rôle d'exécution ECS (pull image, logs, secrets)."
  value       = aws_iam_role.ecs_execution.arn
}

output "ecs_task_role_arn" {
  description = "Rôle d'exécution du code applicatif."
  value       = aws_iam_role.ecs_task.arn
}

output "redis_auth_token" {
  description = "AUTH token Redis (sensible, transmis au module elasticache)."
  value       = random_password.redis_auth.result
  sensitive   = true
}

output "mongo_app_password" {
  description = "Mot de passe de l'utilisateur applicatif MongoDB (sensible)."
  value       = random_password.mongo_app.result
  sensitive   = true
}

output "secret_arns" {
  description = "ARNs des secrets injectés dans les tâches ECS."
  value = {
    redis_auth     = aws_secretsmanager_secret.redis_auth.arn
    mongo_app      = aws_secretsmanager_secret.mongo_app.arn
    dashboard_auth = aws_secretsmanager_secret.dashboard_auth.arn
  }
}
