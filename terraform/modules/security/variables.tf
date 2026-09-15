variable "name" {
  description = "Préfixe de nommage des ressources."
  type        = string
}

variable "region" {
  description = "Région AWS (ARNs des policies)."
  type        = string
}

variable "vpc_id" {
  description = "VPC dans lequel créer les security groups."
  type        = string
}

variable "datalake_bucket_arn" {
  description = "ARN du bucket data lake auquel les tâches applicatives accèdent."
  type        = string
}

variable "tags" {
  description = "Tags communs."
  type        = map(string)
  default     = {}
}
