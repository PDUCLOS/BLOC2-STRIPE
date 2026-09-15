variable "name" {
  description = "Préfixe de nommage (projet Atlas)."
  type        = string
}

variable "atlas_org_id" {
  description = "Identifiant de l'organisation MongoDB Atlas."
  type        = string
}

variable "atlas_region" {
  description = "Région au format Atlas (ex. EU_WEST_1)."
  type        = string
}

variable "aws_region" {
  description = "Région au format AWS (ex. eu-west-1) pour PrivateLink."
  type        = string
}

variable "instance_size" {
  description = "Taille d'instance Atlas (M10 minimum pour PrivateLink)."
  type        = string
  default     = "M10"
}

variable "vpc_id" {
  description = "VPC où créer l'endpoint PrivateLink."
  type        = string
}

variable "subnet_ids" {
  description = "Sous-réseaux data."
  type        = list(string)
}

variable "security_group_id" {
  description = "Security group mongo."
  type        = string
}

variable "app_password" {
  description = "Mot de passe de stripe_app (généré par le module security)."
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "Tags communs (ressources AWS)."
  type        = map(string)
  default     = {}
}
