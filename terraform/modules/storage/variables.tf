variable "name" {
  description = "Préfixe de nommage."
  type        = string
}

variable "kms_key_arn" {
  description = "CMK de chiffrement SSE-KMS."
  type        = string
}

variable "force_destroy" {
  description = "Autorise la suppression d'un bucket non vide (true en dev uniquement)."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags communs."
  type        = map(string)
  default     = {}
}
