# =============================================================================
# Bootstrap — bucket S3 du state Terraform (à appliquer UNE fois, state local)
#
#   cd terraform/bootstrap && terraform init && terraform apply
#
# Ensuite, envs/dev et envs/prod stockent leur state dans ce bucket.
# =============================================================================

terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  description = "Région du bucket de state."
  type        = string
  default     = "eu-west-1"
}

variable "state_bucket_name" {
  description = "Nom du bucket de state (doit correspondre à backend.tf)."
  type        = string
  default     = "stripe-polyglot-tfstate"
}

resource "aws_s3_bucket" "state" {
  bucket = var.state_bucket_name

  lifecycle {
    prevent_destroy = true # perdre le state = perdre la maîtrise de l'infra
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled" # permet de revenir à un state antérieur
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms" # le state contient des valeurs sensibles (mots de passe générés)
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "state_bucket" {
  description = "Bucket à référencer dans envs/*/backend.tf."
  value       = aws_s3_bucket.state.bucket
}
