variable "name" {
  description = "Préfixe de nommage."
  type        = string
}

variable "region" {
  description = "Région AWS."
  type        = string
}

variable "subnet_ids" {
  description = "Sous-réseaux applicatifs privés (les 2 premiers sont utilisés)."
  type        = list(string)
}

variable "security_group_id" {
  description = "Security group applicatif (auto-référencé pour les workers)."
  type        = string
}

variable "dags_bucket_arn" {
  description = "Bucket S3 contenant dags/ et requirements.txt."
  type        = string
}

variable "kms_key_arn" {
  description = "CMK de chiffrement MWAA."
  type        = string
}

variable "airflow_version" {
  description = "Version Airflow (le PoC utilise 2.9)."
  type        = string
  default     = "2.9.2"
}

variable "environment_class" {
  description = "Taille MWAA (mw1.small suffit pour un DAG quotidien)."
  type        = string
  default     = "mw1.small"
}

variable "tags" {
  description = "Tags communs."
  type        = map(string)
  default     = {}
}
