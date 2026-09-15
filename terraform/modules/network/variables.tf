variable "name" {
  description = "Préfixe de nommage des ressources (ex. stripe-polyglot-prod)."
  type        = string
}

variable "region" {
  description = "Région AWS, utilisée pour le nom de service des endpoints VPC."
  type        = string
}

variable "cidr_block" {
  description = "Plage CIDR du VPC. Un /16 laisse de la marge pour 9 sous-réseaux /20."
  type        = string
  default     = "10.20.0.0/16"
}

variable "azs" {
  description = "Zones de disponibilité utilisées (2 minimum pour RDS Multi-AZ, 3 pour MSK)."
  type        = list(string)

  validation {
    condition     = length(var.azs) >= 2 && length(var.azs) <= 3
    error_message = "Entre 2 et 3 zones de disponibilité."
  }
}

variable "single_nat_gateway" {
  description = "true = un seul NAT partagé (dev, économique) ; false = un NAT par AZ (prod, résilient)."
  type        = bool
  default     = false
}

variable "kms_key_arn" {
  description = "Clé KMS de chiffrement des logs CloudWatch."
  type        = string
}

variable "flow_logs_retention_days" {
  description = "Rétention des VPC Flow Logs (PCI-DSS exige 1 an d'historique d'audit)."
  type        = number
  default     = 365
}

variable "tags" {
  description = "Tags communs appliqués à toutes les ressources."
  type        = map(string)
  default     = {}
}
