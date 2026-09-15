variable "project" {
  description = "Nom du projet (tag Project, préfixe des ressources)."
  type        = string
  default     = "stripe-polyglot"
}

variable "environment" {
  description = "Environnement : dev ou prod."
  type        = string

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment doit valoir dev ou prod."
  }
}

variable "region" {
  description = "Région AWS (eu-west-1 : données de paiement européennes hébergées dans l'UE, RGPD)."
  type        = string
  default     = "eu-west-1"
}

variable "azs" {
  description = "Zones de disponibilité."
  type        = list(string)
}

variable "vpc_cidr" {
  description = "CIDR du VPC."
  type        = string
}

variable "single_nat_gateway" {
  description = "Un seul NAT (dev) ou un par AZ (prod)."
  type        = bool
}

variable "rds_instance_class" {
  description = "Classe d'instance RDS."
  type        = string
}

variable "rds_multi_az" {
  description = "RDS Multi-AZ."
  type        = bool
}

variable "rds_read_replica_count" {
  description = "Nombre de réplicas de lecture."
  type        = number
}

variable "msk_broker_count" {
  description = "Nombre de brokers MSK."
  type        = number
}

variable "msk_broker_instance_type" {
  description = "Type d'instance broker MSK."
  type        = string
}

variable "redis_node_type" {
  description = "Type de nœud ElastiCache."
  type        = string
}

variable "redis_num_nodes" {
  description = "Nombre de nœuds Redis."
  type        = number
}

variable "atlas_org_id" {
  description = "Organisation MongoDB Atlas."
  type        = string
}

variable "atlas_region" {
  description = "Région Atlas (EU_WEST_1)."
  type        = string
  default     = "EU_WEST_1"
}

variable "atlas_instance_size" {
  description = "Taille du cluster Atlas."
  type        = string
}

variable "image_tag" {
  description = "Tag de l'image applicative déployée."
  type        = string
  default     = "latest"
}

variable "scorer_desired_count" {
  description = "Nombre de tâches scorer."
  type        = number
}

variable "alert_email" {
  description = "Destinataire des alarmes et alertes budget."
  type        = string
}

variable "monthly_budget_usd" {
  description = "Budget mensuel en USD (cf. docs/FINOPS.md)."
  type        = number
}

variable "tags" {
  description = "Tags communs."
  type        = map(string)
  default     = {}
}
