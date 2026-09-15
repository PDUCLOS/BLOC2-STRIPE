# =============================================================================
# Module storage — buckets S3
#
#   - datalake : archive des faits > 2 ans en Parquet / Iceberg
#     (docs/OLAP_SCHEMA_DESIGN.md), exports batch, artefacts MLflow
#     (équivalent du volume mlflow-artifacts et de ml/models/*.pkl du PoC).
#   - airflow  : DAGs et requirements pour MWAA (équivalent du montage
#     ./dags du service `airflow` de docker-compose.yml).
# Tous : chiffrement SSE-KMS, versioning, accès public bloqué, TLS obligatoire.
# =============================================================================

data "aws_caller_identity" "current" {}

locals {
  # Suffixe compte : les noms de bucket S3 sont globaux à tout AWS.
  buckets = {
    datalake = "${var.name}-datalake-${data.aws_caller_identity.current.account_id}"
    airflow  = "${var.name}-airflow-${data.aws_caller_identity.current.account_id}"
  }
}

resource "aws_s3_bucket" "this" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = var.force_destroy
  tags          = merge(var.tags, { Purpose = each.key })
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled" # MWAA l'exige ; protège aussi le data lake d'un écrasement
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true # divise le coût des appels KMS
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "tls_only" {
  for_each = aws_s3_bucket.this

  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [each.value.arn, "${each.value.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  policy   = data.aws_iam_policy_document.tls_only[each.key].json

  depends_on = [aws_s3_bucket_public_access_block.this]
}

# Cycle de vie du data lake (GreenOps / FinOps) : les faits archivés ne sont
# presque jamais relus → classes de stockage de moins en moins chères.
resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.this["datalake"].id

  rule {
    id     = "archive-facts"
    status = "Enabled"
    filter {
      prefix = "archive/"
    }
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 365
      storage_class = "GLACIER_IR"
    }
  }

  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}
