# =============================================================================
# Module compute — ECR + ECS Fargate pour les processus Python du PoC
#
# Chaque service ci-dessous correspond à un processus qui tourne aujourd'hui
# en local (sur l'hôte ou en conteneur) :
#   scorer       → producers/flink_like_job.py   (consumer CDC, scoring, write-back)
#   mongo-writer → producers/mongo_writer.py     (Kafka → MongoDB)
#   ml-monitor   → ml/monitor.py                 (service `ml-monitor`, Evidently)
#   dashboard    → dashboard/app.py              (service `dashboard`, Streamlit)
# Le scorer est la cible d'un remplacement par Managed Service for Apache
# Flink (flink/fraud_scoring_job.py) quand le débit le justifie ; Fargate
# garde ici la même logique métier sans réécriture.
# =============================================================================

locals {
  services = {
    scorer = {
      cpu     = 1024
      memory  = 2048
      command = ["python", "-u", "producers/flink_like_job.py"]
      count   = var.scorer_desired_count
    }
    mongo-writer = {
      cpu     = 512
      memory  = 1024
      command = ["python", "-u", "producers/mongo_writer.py"]
      count   = 1
    }
    ml-monitor = {
      cpu     = 1024
      memory  = 2048
      command = ["python", "-u", "ml/monitor.py"]
      count   = 1
    }
    dashboard = {
      cpu     = 512
      memory  = 1024
      command = ["streamlit", "run", "dashboard/app.py", "--server.port", "8501"]
      count   = 1
    }
  }

  # Variables non sensibles communes (mêmes noms que .env.example : le code
  # applicatif n'a pas à changer entre local et cloud).
  common_environment = [
    { name = "PG_HOST", value = var.pg_host },
    { name = "PG_DB", value = "stripe_oltp" },
    { name = "KAFKA_BROKERS", value = var.kafka_bootstrap },
    { name = "REDIS_HOST", value = var.redis_host },
    { name = "SCORING_ENGINE", value = "ml" },
    { name = "PGSSLMODE", value = "require" },
  ]

  # Secrets injectés par l'agent ECS depuis Secrets Manager au démarrage :
  # ils n'apparaissent jamais dans la définition de tâche ni dans les logs.
  common_secrets = [
    { name = "PG_PASSWORD", valueFrom = "${var.pg_master_secret_arn}:password::" },
    { name = "PG_USER", valueFrom = "${var.pg_master_secret_arn}:username::" },
    { name = "REDIS_PASSWORD", valueFrom = var.secret_arns["redis_auth"] },
    { name = "MONGO_APP_PASSWORD", valueFrom = "${var.secret_arns["mongo_app"]}:password::" },
    { name = "MONGO_APP_USER", valueFrom = "${var.secret_arns["mongo_app"]}:username::" },
  ]
}

resource "aws_ecr_repository" "app" {
  name                 = "${var.name}/app"
  image_tag_mutability = "IMMUTABLE" # un tag = une image, traçabilité des déploiements

  image_scanning_configuration {
    scan_on_push = true # CVE scan à chaque push (CI GitHub Actions)
  }

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = var.kms_key_arn
  }

  tags = var.tags
}

resource "aws_ecr_lifecycle_policy" "app" {
  repository = aws_ecr_repository.app.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Garde les 20 dernieres images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 20
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_ecs_cluster" "this" {
  name = "${var.name}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "service" {
  for_each          = local.services
  name              = "/ecs/${var.name}/${each.key}"
  retention_in_days = 90 # aligné sur le TTL RGPD des transaction_logs Mongo
  kms_key_id        = var.kms_key_arn
  tags              = var.tags
}

resource "aws_ecs_task_definition" "service" {
  for_each = local.services

  family                   = "${var.name}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64" # Graviton : ~20 % moins cher, même image que le Mac M-series du PoC
  }

  container_definitions = jsonencode([{
    name        = each.key
    image       = "${aws_ecr_repository.app.repository_url}:${var.image_tag}"
    command     = each.value.command
    essential   = true
    environment = local.common_environment
    secrets     = local.common_secrets
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.service[each.key].name
        awslogs-region        = var.region
        awslogs-stream-prefix = each.key
      }
    }
  }])

  tags = var.tags
}

resource "aws_ecs_service" "service" {
  for_each = local.services

  name            = each.key
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.service[each.key].arn
  desired_count   = each.value.count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [var.app_security_group_id]
    assign_public_ip = false
  }

  # Rolling update sans coupure : la nouvelle tâche démarre avant l'arrêt de
  # l'ancienne. Le write-back idempotent (fraud_score IS NULL) tolère que deux
  # scorers traitent brièvement le même message pendant la bascule.
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  tags = var.tags
}
