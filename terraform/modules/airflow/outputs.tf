output "webserver_url" {
  description = "URL privée de l'interface Airflow."
  value       = aws_mwaa_environment.this.webserver_url
}

output "execution_role_arn" {
  description = "Rôle d'exécution MWAA."
  value       = aws_iam_role.execution.arn
}
