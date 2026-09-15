# =============================================================================
# Module airflow — Amazon MWAA (Airflow managé)
#
# Équivalent du service `airflow` (profil Docker "airflow") qui exécute
# dags/stripe_daily_etl.py : export quotidien PostgreSQL → Snowflake à 02:00 UTC
# puis REFRESH des vues matérialisées. Les DAGs sont synchronisés dans le
# bucket S3 par la CI (aws s3 sync dags/ s3://<bucket>/dags/).
# =============================================================================

data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.name}-mwaa-execution"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "execution" {
  statement {
    sid       = "ReadDags"
    actions   = ["s3:GetObject*", "s3:GetBucket*", "s3:List*"]
    resources = [var.dags_bucket_arn, "${var.dags_bucket_arn}/*"]
  }
  statement {
    sid = "Logs"
    actions = [
      "logs:CreateLogStream",
      "logs:CreateLogGroup",
      "logs:PutLogEvents",
      "logs:GetLogEvents",
      "logs:GetLogRecord",
      "logs:GetLogGroupFields",
      "logs:GetQueryResults",
      "logs:DescribeLogGroups",
    ]
    resources = ["arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:airflow-${var.name}-*"]
  }
  statement {
    sid       = "Metrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
  statement {
    sid       = "CeleryQueues"
    actions   = ["sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:SendMessage"]
    resources = ["arn:aws:sqs:${var.region}:*:airflow-celery-*"]
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey*", "kms:Encrypt"]
    resources = [var.kms_key_arn]
  }
  # Credentials Snowflake et Postgres lus au runtime par le DAG
  # (variables SNOWFLAKE_* et PG_* du .env local).
  statement {
    sid       = "ReadEtlSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:${var.name}/*"]
  }
}

resource "aws_iam_role_policy" "execution" {
  name   = "mwaa-execution"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution.json
}

resource "aws_mwaa_environment" "this" {
  name              = "${var.name}-airflow"
  airflow_version   = var.airflow_version
  environment_class = var.environment_class
  max_workers       = 2 # un seul DAG quotidien : pas besoin de plus

  source_bucket_arn    = var.dags_bucket_arn
  dag_s3_path          = "dags/"
  requirements_s3_path = "requirements.txt"

  execution_role_arn = aws_iam_role.execution.arn
  kms_key            = var.kms_key_arn

  # Interface web accessible uniquement depuis le VPC (VPN / bastion).
  webserver_access_mode = "PRIVATE_ONLY"

  network_configuration {
    security_group_ids = [var.security_group_id]
    subnet_ids         = slice(var.subnet_ids, 0, 2) # MWAA exige exactement 2 sous-réseaux
  }

  logging_configuration {
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "WARNING"
    }
  }

  tags = var.tags
}
