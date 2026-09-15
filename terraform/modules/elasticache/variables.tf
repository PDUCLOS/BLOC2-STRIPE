variable "name" {
  description = "Préfixe de nommage."
  type        = string
}

variable "subnet_ids" {
  description = "Sous-réseaux data."
  type        = list(string)
}

variable "security_group_id" {
  description = "Security group Redis (6379 depuis le SG applicatif)."
  type        = string
}

variable "kms_key_arn" {
  description = "CMK de chiffrement au repos."
  type        = string
}

variable "auth_token" {
  description = "AUTH token Redis (généré par le module security)."
  type        = string
  sensitive   = true
}

variable "engine_version" {
  description = "Version Redis (alignée sur redis:7 du PoC)."
  type        = string
  default     = "7.1"
}

variable "node_type" {
  description = "Type de nœud (cache.t4g.small en dev, cache.r7g.large en prod)."
  type        = string
}

variable "num_cache_clusters" {
  description = "Nombre de nœuds (1 = pas de réplica, 2+ = Multi-AZ)."
  type        = number
  default     = 2
}

variable "tags" {
  description = "Tags communs."
  type        = map(string)
  default     = {}
}
