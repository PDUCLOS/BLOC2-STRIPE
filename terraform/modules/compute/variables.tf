variable "name" {
  description = "Préfixe de nommage."
  type        = string
}

variable "region" {
  description = "Région AWS (driver de logs)."
  type        = string
}

variable "subnet_ids" {
  description = "Sous-réseaux applicatifs privés."
  type        = list(string)
}

variable "app_security_group_id" {
  description = "Security group applicatif."
  type        = string
}

variable "kms_key_arn" {
  description = "CMK (ECR, logs)."
  type        = string
}

variable "execution_role_arn" {
  description = "Rôle d'exécution ECS."
  type        = string
}

variable "task_role_arn" {
  description = "Rôle applicatif ECS."
  type        = string
}

variable "image_tag" {
  description = "Tag de l'image applicative à déployer (SHA git poussé par la CI)."
  type        = string
  default     = "latest"
}

variable "scorer_desired_count" {
  description = "Nombre de scorers (≤ nombre de partitions de stripe.public.transactions)."
  type        = number
  default     = 2
}

variable "pg_host" {
  description = "Endpoint PostgreSQL primaire."
  type        = string
}

variable "pg_master_secret_arn" {
  description = "Secret RDS (username/password)."
  type        = string
}

variable "kafka_bootstrap" {
  description = "Bootstrap brokers MSK (IAM)."
  type        = string
}

variable "redis_host" {
  description = "Endpoint primaire ElastiCache."
  type        = string
}

variable "secret_arns" {
  description = "ARNs des secrets applicatifs (clés redis_auth, mongo_app, dashboard_auth)."
  type        = map(string)
}

variable "tags" {
  description = "Tags communs."
  type        = map(string)
  default     = {}
}
