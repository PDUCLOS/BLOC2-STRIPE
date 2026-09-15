variable "region" {
  description = "Région AWS."
  type        = string
  default     = "eu-west-1"
}

variable "atlas_org_id" {
  description = "Organisation MongoDB Atlas."
  type        = string
}

variable "alert_email" {
  description = "Destinataire des alarmes."
  type        = string
}

variable "image_tag" {
  description = "Tag de l'image applicative (SHA git)."
  type        = string
  default     = "latest"
}
