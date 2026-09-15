# =============================================================================
# Module elasticache — Redis managé, feature store "online" du scorer
#
# Côté PoC : service `redis` de docker-compose.yml. Le scorer
# (producers/flink_like_job.py) y écrit/lit :
#   - v1h_<customer_id>, v24h_<customer_id> : ZSET de vélocité (fenêtres glissantes)
#   - feat_<customer_id>                     : HASH des dernières features
# Ces clés ont un TTL court et sont reconstructibles depuis Postgres
# (_backfill_velocity) : la persistance n'est donc pas critique, mais la
# disponibilité l'est (le scoring se dégrade sans vélocité) → Multi-AZ.
# =============================================================================

resource "aws_elasticache_subnet_group" "this" {
  name       = "${var.name}-redis"
  subnet_ids = var.subnet_ids
  tags       = var.tags
}

resource "aws_elasticache_parameter_group" "this" {
  name   = "${var.name}-redis7"
  family = "redis7"
  tags   = var.tags

  # Les clés de vélocité ont toutes un TTL : on évince d'abord celles-ci
  # si la mémoire sature, plutôt que de refuser les écritures du scorer.
  parameter {
    name  = "maxmemory-policy"
    value = "volatile-lru"
  }
}

resource "aws_elasticache_replication_group" "this" {
  replication_group_id = "${var.name}-redis"
  description          = "Feature store online (velocite) du scorer fraude"

  engine               = "redis"
  engine_version       = var.engine_version
  node_type            = var.node_type
  num_cache_clusters   = var.num_cache_clusters
  parameter_group_name = aws_elasticache_parameter_group.this.name
  port                 = 6379

  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [var.security_group_id]

  # Primaire + réplica(s) dans d'autres AZ, bascule automatique.
  automatic_failover_enabled = var.num_cache_clusters > 1
  multi_az_enabled           = var.num_cache_clusters > 1

  at_rest_encryption_enabled = true
  kms_key_id                 = var.kms_key_arn
  transit_encryption_enabled = true
  auth_token                 = var.auth_token # équivalent de REDIS_PASSWORD du .env

  snapshot_retention_limit = 1
  snapshot_window          = "02:30-03:30"
  maintenance_window       = "sun:04:00-sun:05:00"

  tags = var.tags
}
