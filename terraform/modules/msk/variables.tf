variable "name" {
  description = "Préfixe de nommage."
  type        = string
}

variable "subnet_ids" {
  description = "Sous-réseaux data, un par AZ (le nombre de brokers doit en être un multiple)."
  type        = list(string)
}

variable "security_group_id" {
  description = "Security group MSK (9098 depuis le SG applicatif)."
  type        = string
}

variable "kms_key_arn" {
  description = "CMK de chiffrement au repos."
  type        = string
}

variable "kafka_version" {
  description = "Version Kafka (le PoC tourne Confluent 7.x ≈ Kafka 3.x)."
  type        = string
  default     = "3.6.0"
}

variable "broker_count" {
  description = "Nombre de brokers (multiple du nombre d'AZ)."
  type        = number
  default     = 3
}

variable "broker_instance_type" {
  description = "Type d'instance broker."
  type        = string
  default     = "kafka.m7g.large"
}

variable "broker_volume_gb" {
  description = "Volume EBS par broker en Go."
  type        = number
  default     = 500
}

variable "tags" {
  description = "Tags communs."
  type        = map(string)
  default     = {}
}
