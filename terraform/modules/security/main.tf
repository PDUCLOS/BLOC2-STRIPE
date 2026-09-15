# =============================================================================
# Module security — KMS, Secrets Manager, security groups, IAM
#
# Transpose en cloud ce que le PoC fait localement :
#   - .env généré par scripts/init_env.sh      → AWS Secrets Manager (chiffré KMS)
#   - rôles Postgres replication_user / analytics_reader
#     (scripts/postgres_init_roles.sh)          → rôles IAM + security groups dédiés
#   - pgcrypto / fingerprint SHA-256           → chiffrement au repos KMS partout
# Référence : docs/SECURITY_COMPLIANCE_PLAN.md (§2 chiffrement, §3 IAM).
# =============================================================================

data "aws_caller_identity" "current" {}

# --- Clé KMS unique gérée par le client (CMK) --------------------------------
# Une seule CMK pour toute la stack : rotation annuelle automatique, et une
# révocation de la clé rend illisibles RDS, MSK, Redis, S3, logs et secrets.
# En prod à grande échelle, on séparerait une clé par domaine (données de
# paiement vs logs) pour limiter le rayon d'impact.

data "aws_iam_policy_document" "kms" {
  statement {
    sid       = "RootAccountAdmin"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }

  # CloudWatch Logs doit pouvoir chiffrer les log groups (flow logs, ECS, MSK).
  statement {
    sid = "CloudWatchLogs"
    actions = [
      "kms:Encrypt*",
      "kms:Decrypt*",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:Describe*",
    ]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["logs.${var.region}.amazonaws.com"]
    }
  }
}

resource "aws_kms_key" "this" {
  description             = "CMK ${var.name} — RDS, MSK, ElastiCache, S3, Secrets, Logs"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.kms.json
  tags                    = var.tags
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.name}"
  target_key_id = aws_kms_key.this.key_id
}

# --- Secrets applicatifs -----------------------------------------------------
# Équivalents cloud des variables REDIS_PASSWORD / MONGO_APP_PASSWORD /
# DASHBOARD_PASSWORD_HASH du .env local. Le mot de passe maître RDS n'est PAS
# ici : il est géré par RDS lui-même (manage_master_user_password, cf. module rds).

resource "random_password" "redis_auth" {
  length  = 48
  special = false # ElastiCache refuse certains caractères spéciaux dans l'AUTH token
}

resource "random_password" "mongo_app" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "redis_auth" {
  name       = "${var.name}/redis/auth-token"
  kms_key_id = aws_kms_key.this.arn
  tags       = var.tags
}

resource "aws_secretsmanager_secret_version" "redis_auth" {
  secret_id     = aws_secretsmanager_secret.redis_auth.id
  secret_string = random_password.redis_auth.result
}

resource "aws_secretsmanager_secret" "mongo_app" {
  name       = "${var.name}/mongodb/app-user"
  kms_key_id = aws_kms_key.this.arn
  tags       = var.tags
}

resource "aws_secretsmanager_secret_version" "mongo_app" {
  secret_id = aws_secretsmanager_secret.mongo_app.id
  secret_string = jsonencode({
    username = "stripe_app"
    password = random_password.mongo_app.result
  })
}

# Hash du mot de passe du dashboard : renseigné hors Terraform (rotation
# manuelle), Terraform ne crée que le conteneur du secret.
resource "aws_secretsmanager_secret" "dashboard_auth" {
  name       = "${var.name}/dashboard/auth"
  kms_key_id = aws_kms_key.this.arn
  tags       = var.tags
}

# --- Security groups (moindre privilège réseau) ------------------------------
# Principe : les services data n'acceptent QUE le security group applicatif,
# jamais un CIDR. Aucun port data n'est ouvert à 0.0.0.0/0.

resource "aws_security_group" "app" {
  name        = "${var.name}-app"
  description = "Taches ECS Fargate (scorer, mongo-writer, dashboard, ml-monitor) et MWAA"
  vpc_id      = var.vpc_id
  tags        = merge(var.tags, { Name = "${var.name}-app" })
}

resource "aws_vpc_security_group_egress_rule" "app_all" {
  security_group_id = aws_security_group.app.id
  description       = "Sortie vers les services data et les APIs AWS (via NAT / endpoints)"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# Auto-référence : les workers MWAA doivent se parler entre eux.
resource "aws_vpc_security_group_ingress_rule" "app_self" {
  security_group_id            = aws_security_group.app.id
  description                  = "Trafic interne entre taches applicatives (MWAA workers)"
  ip_protocol                  = "-1"
  referenced_security_group_id = aws_security_group.app.id
}

locals {
  # service → port d'écoute. Chaque entrée produit un security group dédié
  # n'autorisant que ce port, et uniquement depuis le SG applicatif.
  data_ports = {
    rds   = 5432  # PostgreSQL (OLTP)
    msk   = 9098  # Kafka, authentification IAM sur TLS
    redis = 6379  # ElastiCache (feature store vélocité)
    mongo = 27017 # MongoDB Atlas via PrivateLink (Atlas utilise ensuite 1024-65535)
  }
}

resource "aws_security_group" "data" {
  for_each    = local.data_ports
  name        = "${var.name}-${each.key}"
  description = "Acces ${each.key} depuis le SG applicatif uniquement"
  vpc_id      = var.vpc_id
  tags        = merge(var.tags, { Name = "${var.name}-${each.key}" })
}

resource "aws_vpc_security_group_ingress_rule" "data_from_app" {
  for_each                     = { for k, v in local.data_ports : k => v if k != "mongo" }
  security_group_id            = aws_security_group.data[each.key].id
  description                  = "${each.key} depuis les taches applicatives"
  ip_protocol                  = "tcp"
  from_port                    = each.value
  to_port                      = each.value
  referenced_security_group_id = aws_security_group.app.id
}

# Atlas PrivateLink attribue un port par nœud du replica set dans cette plage.
resource "aws_vpc_security_group_ingress_rule" "mongo_from_app" {
  security_group_id            = aws_security_group.data["mongo"].id
  description                  = "MongoDB Atlas PrivateLink depuis les taches applicatives"
  ip_protocol                  = "tcp"
  from_port                    = 1024
  to_port                      = 65535
  referenced_security_group_id = aws_security_group.app.id
}

# --- IAM : rôles des tâches ECS ----------------------------------------------

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Rôle d'EXÉCUTION : utilisé par l'agent ECS (pull ECR, logs, injection des secrets).
resource "aws_iam_role" "ecs_execution" {
  name               = "${var.name}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "ecs_execution_managed" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "ecs_execution_secrets" {
  statement {
    sid       = "ReadAppSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:${var.name}/*"]
  }
  statement {
    sid       = "DecryptWithStackKey"
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.this.arn]
  }
}

resource "aws_iam_role_policy" "ecs_execution_secrets" {
  name   = "read-app-secrets"
  role   = aws_iam_role.ecs_execution.id
  policy = data.aws_iam_policy_document.ecs_execution_secrets.json
}

# Rôle de TÂCHE : ce que le code Python a le droit de faire à l'exécution.
# Kafka via IAM (plus de mot de passe Kafka), lecture/écriture du data lake,
# lecture du modèle entraîné (équivalent de ml/models/*.pkl).
resource "aws_iam_role" "ecs_task" {
  name               = "${var.name}-ecs-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "ecs_task" {
  statement {
    sid = "MskIamAuth"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:WriteData",
      "kafka-cluster:DescribeGroup",
      "kafka-cluster:AlterGroup",
    ]
    resources = [
      "arn:aws:kafka:${var.region}:${data.aws_caller_identity.current.account_id}:cluster/${var.name}-msk/*",
      "arn:aws:kafka:${var.region}:${data.aws_caller_identity.current.account_id}:topic/${var.name}-msk/*/stripe.*",
      "arn:aws:kafka:${var.region}:${data.aws_caller_identity.current.account_id}:group/${var.name}-msk/*/*",
    ]
  }
  statement {
    sid       = "DataLakeReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = [var.datalake_bucket_arn, "${var.datalake_bucket_arn}/*"]
  }
  statement {
    sid       = "UseStackKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.this.arn]
  }
}

resource "aws_iam_role_policy" "ecs_task" {
  name   = "stripe-app-runtime"
  role   = aws_iam_role.ecs_task.id
  policy = data.aws_iam_policy_document.ecs_task.json
}
