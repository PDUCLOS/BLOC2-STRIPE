# =============================================================================
# Stack Stripe Polyglot — composition de tous les modules
#
# Appelée par terraform/envs/dev et terraform/envs/prod, qui ne font que
# choisir les tailles. Ordre des dépendances (résolu par Terraform) :
#   security (KMS) ← storage ← network ← rds / msk / elasticache / atlas
#   ← compute (ECS) / airflow (MWAA) ← observability (alarmes, budget)
#
# Correspondance PoC local → cible AWS :
#   postgres (compose)      → module.rds          (RDS PostgreSQL 16 Multi-AZ)
#   kafka + debezium        → module.msk          (MSK 3 AZ ; Debezium sur MSK Connect, hors périmètre)
#   redis                   → module.elasticache  (ElastiCache Redis 7 Multi-AZ)
#   mongo                   → module.mongodb_atlas (Atlas M10+ via PrivateLink)
#   producers/*, ml-monitor, dashboard → module.compute (ECS Fargate ARM64)
#   airflow                 → module.airflow      (MWAA)
#   mlflow-artifacts, archive → module.storage    (S3 SSE-KMS)
#   .env / init_env.sh      → module.security     (Secrets Manager + KMS + IAM)
# Snowflake (OLAP) reste provisionné par etl/snowflake_setup.py : son provider
# Terraform est volontairement laissé hors de cette stack AWS.
# =============================================================================

locals {
  name = "${var.project}-${var.environment}"
}

module "security" {
  source = "../modules/security"

  name                = local.name
  region              = var.region
  vpc_id              = module.network.vpc_id
  datalake_bucket_arn = module.storage.datalake_bucket_arn
  tags                = var.tags
}

module "storage" {
  source = "../modules/storage"

  name          = local.name
  kms_key_arn   = module.security.kms_key_arn
  force_destroy = var.environment != "prod"
  tags          = var.tags
}

module "network" {
  source = "../modules/network"

  name               = local.name
  region             = var.region
  cidr_block         = var.vpc_cidr
  azs                = var.azs
  single_nat_gateway = var.single_nat_gateway
  kms_key_arn        = module.security.kms_key_arn
  tags               = var.tags
}

module "rds" {
  source = "../modules/rds"

  name                  = local.name
  subnet_ids            = module.network.data_subnet_ids
  security_group_id     = module.security.data_security_group_ids["rds"]
  kms_key_arn           = module.security.kms_key_arn
  instance_class        = var.rds_instance_class
  multi_az              = var.rds_multi_az
  read_replica_count    = var.rds_read_replica_count
  backup_retention_days = var.environment == "prod" ? 35 : 3
  deletion_protection   = var.environment == "prod"
  tags                  = var.tags
}

module "msk" {
  source = "../modules/msk"

  name                 = local.name
  subnet_ids           = module.network.data_subnet_ids
  security_group_id    = module.security.data_security_group_ids["msk"]
  kms_key_arn          = module.security.kms_key_arn
  broker_count         = var.msk_broker_count
  broker_instance_type = var.msk_broker_instance_type
  tags                 = var.tags
}

module "elasticache" {
  source = "../modules/elasticache"

  name               = local.name
  subnet_ids         = module.network.data_subnet_ids
  security_group_id  = module.security.data_security_group_ids["redis"]
  kms_key_arn        = module.security.kms_key_arn
  auth_token         = module.security.redis_auth_token
  node_type          = var.redis_node_type
  num_cache_clusters = var.redis_num_nodes
  tags               = var.tags
}

module "mongodb_atlas" {
  source = "../modules/mongodb_atlas"

  name              = local.name
  atlas_org_id      = var.atlas_org_id
  atlas_region      = var.atlas_region
  aws_region        = var.region
  instance_size     = var.atlas_instance_size
  vpc_id            = module.network.vpc_id
  subnet_ids        = module.network.data_subnet_ids
  security_group_id = module.security.data_security_group_ids["mongo"]
  app_password      = module.security.mongo_app_password
  tags              = var.tags
}

module "compute" {
  source = "../modules/compute"

  name                  = local.name
  region                = var.region
  subnet_ids            = module.network.app_subnet_ids
  app_security_group_id = module.security.app_security_group_id
  kms_key_arn           = module.security.kms_key_arn
  execution_role_arn    = module.security.ecs_execution_role_arn
  task_role_arn         = module.security.ecs_task_role_arn
  image_tag             = var.image_tag
  scorer_desired_count  = var.scorer_desired_count
  pg_host               = module.rds.endpoint
  pg_master_secret_arn  = module.rds.master_user_secret_arn
  kafka_bootstrap       = module.msk.bootstrap_brokers_iam
  redis_host            = module.elasticache.primary_endpoint
  secret_arns           = module.security.secret_arns
  tags                  = var.tags
}

module "airflow" {
  source = "../modules/airflow"

  name              = local.name
  region            = var.region
  subnet_ids        = module.network.app_subnet_ids
  security_group_id = module.security.app_security_group_id
  dags_bucket_arn   = module.storage.airflow_bucket_arn
  kms_key_arn       = module.security.kms_key_arn
  tags              = var.tags
}

# =============================================================================
# Observabilité & FinOps
# =============================================================================

resource "aws_sns_topic" "alerts" {
  name              = "${local.name}-alerts"
  kms_master_key_id = module.security.kms_key_arn
  tags              = var.tags
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# CPU du primaire RDS : un OLTP saturé ralentit l'autorisation des paiements.
resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${local.name}-rds-cpu-high"
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  dimensions          = { DBInstanceIdentifier = module.rds.instance_id }
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = 80
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

# Retard de consommation du scorer : au-delà, le SLA "scoring < 100 ms"
# n'est plus tenu (même signal que le taux affiché par flink_like_job.py).
resource "aws_cloudwatch_metric_alarm" "scorer_lag" {
  alarm_name          = "${local.name}-scorer-consumer-lag"
  namespace           = "AWS/Kafka"
  metric_name         = "SumOffsetLag"
  dimensions          = { "Cluster Name" = module.msk.cluster_name, "Consumer Group" = "flink-fraud-scorer", Topic = "stripe.public.transactions" }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 1000
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

# Budget mensuel (C8 FinOps) : alerte à 80 % du réel et à 100 % du prévisionnel.
# Le chiffrage détaillé est dans docs/FINOPS.md.
resource "aws_budgets_budget" "monthly" {
  name         = "${local.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:Project$${var.project}"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
