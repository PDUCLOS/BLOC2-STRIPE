variable "name" {
  description = "Préfixe de nommage."
  type        = string
}

variable "subnet_ids" {
  description = "Sous-réseaux data (sans route Internet)."
  type        = list(string)
}

variable "security_group_id" {
  description = "Security group RDS (5432 depuis le SG applicatif uniquement)."
  type        = string
}

variable "kms_key_arn" {
  description = "CMK de chiffrement du stockage, des snapshots et du secret maître."
  type        = string
}

variable "engine_version" {
  description = "Version PostgreSQL (alignée sur postgres:16 du PoC)."
  type        = string
  default     = "16.4"
}

variable "instance_class" {
  description = "Classe d'instance (db.t4g.medium en dev, db.r6g.large+ en prod)."
  type        = string
}

variable "allocated_storage_gb" {
  description = "Stockage initial en Go (autoscaling jusqu'à x5)."
  type        = number
  default     = 100
}

variable "multi_az" {
  description = "Standby synchrone dans une autre AZ."
  type        = bool
  default     = true
}

variable "read_replica_count" {
  description = "Nombre de réplicas de lecture (dashboard, analytics_reader)."
  type        = number
  default     = 1
}

variable "backup_retention_days" {
  description = "Rétention des sauvegardes automatiques (1-35 jours)."
  type        = number
  default     = 14
}

variable "deletion_protection" {
  description = "Empêche un `terraform destroy` accidentel (true en prod)."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags communs."
  type        = map(string)
  default     = {}
}
