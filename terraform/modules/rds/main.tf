# =============================================================================
# Module rds — PostgreSQL 16 OLTP (équivalent du service `postgres` de
# docker-compose.yml, schéma init/postgres/01_ddl.sql)
#
# Paramètres qui comptent pour ce projet :
#   - rds.logical_replication = 1 : indispensable au CDC Debezium (le PoC règle
#     wal_level=logical dans docker-compose.yml, même besoin ici).
#   - Multi-AZ synchrone : failover automatique < 60-120 s (cible "< 30 s"
#     nécessiterait un Multi-AZ DB cluster à 2 standbys lisibles).
#   - Mot de passe maître géré par RDS dans Secrets Manager (rotation auto),
#     jamais dans le state Terraform en clair.
# =============================================================================

resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-pg"
  subnet_ids = var.subnet_ids
  tags       = var.tags
}

resource "aws_db_parameter_group" "this" {
  name   = "${var.name}-pg16"
  family = "postgres16"
  tags   = var.tags

  # CDC Debezium (plugin pgoutput) — cf. config/debezium-connector.json
  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  # Refuse toute connexion non TLS (PCI-DSS req. 4). En local, PGSSLMODE est
  # désactivé et documenté comme écart assumé du PoC.
  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }

  # Trace les requêtes > 500 ms : repère les requêtes analytiques qui
  # devraient partir vers Snowflake plutôt que charger l'OLTP.
  parameter {
    name  = "log_min_duration_statement"
    value = "500"
  }
}

resource "aws_db_instance" "primary" {
  identifier     = "${var.name}-pg"
  engine         = "postgres"
  engine_version = var.engine_version
  instance_class = var.instance_class

  db_name  = "stripe_oltp"
  username = "stripe_admin"
  # RDS génère le mot de passe et le stocke dans Secrets Manager chiffré par la CMK.
  manage_master_user_password   = true
  master_user_secret_kms_key_id = var.kms_key_arn

  allocated_storage     = var.allocated_storage_gb
  max_allocated_storage = var.allocated_storage_gb * 5 # autoscaling du stockage
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id            = var.kms_key_arn

  multi_az               = var.multi_az
  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [var.security_group_id]
  publicly_accessible    = false
  parameter_group_name   = aws_db_parameter_group.this.name

  iam_database_authentication_enabled = true

  backup_retention_period  = var.backup_retention_days
  backup_window            = "01:00-02:00" # avant le DAG Airflow de 02:00 UTC
  maintenance_window       = "sun:03:00-sun:04:00"
  copy_tags_to_snapshot    = true
  delete_automated_backups = false

  performance_insights_enabled          = true
  performance_insights_kms_key_id       = var.kms_key_arn
  performance_insights_retention_period = 7
  enabled_cloudwatch_logs_exports       = ["postgresql"]

  deletion_protection       = var.deletion_protection
  skip_final_snapshot       = !var.deletion_protection
  final_snapshot_identifier = var.deletion_protection ? "${var.name}-pg-final" : null

  tags = var.tags
}

# Réplica de lecture : sert le dashboard et les requêtes analytics_reader
# sans charger le primaire qui encaisse les écritures de paiement.
resource "aws_db_instance" "read_replica" {
  count = var.read_replica_count

  identifier          = "${var.name}-pg-replica-${count.index}"
  replicate_source_db = aws_db_instance.primary.identifier
  instance_class      = var.instance_class

  storage_encrypted      = true
  kms_key_id             = var.kms_key_arn
  vpc_security_group_ids = [var.security_group_id]
  publicly_accessible    = false
  parameter_group_name   = aws_db_parameter_group.this.name

  performance_insights_enabled    = true
  performance_insights_kms_key_id = var.kms_key_arn

  skip_final_snapshot = true
  tags                = var.tags
}
