# =============================================================================
# Module mongodb_atlas — MongoDB Atlas sur AWS, joint en PrivateLink
#
# Équivalent du service `mongo` de docker-compose.yml et des scripts
# init/mongo/01_init_collections.js (collections, index TTL) et
# init/mongo/02_app_user.js (utilisateur applicatif stripe_app).
# Atlas plutôt que DocumentDB : compatibilité MongoDB 7 complète (index TTL,
# $percentile utilisé dans queries/mongodb_queries.js), sauvegarde continue.
# Le trafic ne quitte jamais le réseau AWS : endpoint d'interface PrivateLink
# dans les sous-réseaux data, aucune IP publique autorisée côté Atlas.
# =============================================================================

resource "mongodbatlas_project" "this" {
  name   = var.name
  org_id = var.atlas_org_id
}

resource "mongodbatlas_cluster" "this" {
  project_id   = mongodbatlas_project.this.id
  name         = "${var.name}-nosql"
  cluster_type = "REPLICASET"

  provider_name               = "AWS"
  provider_region_name        = var.atlas_region # ex. EU_WEST_1
  provider_instance_size_name = var.instance_size
  mongo_db_major_version      = "7.0"

  # Replica set 3 nœuds répartis sur 3 AZ par Atlas (tolérance à la perte d'une AZ).
  replication_specs {
    num_shards = 1
    regions_config {
      region_name     = var.atlas_region
      electable_nodes = 3
      priority        = 7
      read_only_nodes = 0
    }
  }

  cloud_backup                 = true # sauvegarde continue + restauration point-in-time
  auto_scaling_disk_gb_enabled = true
}

# --- PrivateLink : service côté Atlas, endpoint côté VPC ---------------------

resource "mongodbatlas_privatelink_endpoint" "this" {
  project_id    = mongodbatlas_project.this.id
  provider_name = "AWS"
  region        = var.aws_region
}

resource "aws_vpc_endpoint" "atlas" {
  vpc_id             = var.vpc_id
  service_name       = mongodbatlas_privatelink_endpoint.this.endpoint_service_name
  vpc_endpoint_type  = "Interface"
  subnet_ids         = var.subnet_ids
  security_group_ids = [var.security_group_id]
  tags               = merge(var.tags, { Name = "${var.name}-vpce-atlas" })
}

resource "mongodbatlas_privatelink_endpoint_service" "this" {
  project_id          = mongodbatlas_project.this.id
  private_link_id     = mongodbatlas_privatelink_endpoint.this.private_link_id
  endpoint_service_id = aws_vpc_endpoint.atlas.id
  provider_name       = "AWS"
}

# --- Utilisateur applicatif (moindre privilège) ------------------------------
# Même rôle que init/mongo/02_app_user.js : readWrite sur stripe_nosql
# uniquement, aucun droit d'administration du cluster.
resource "mongodbatlas_database_user" "app" {
  project_id         = mongodbatlas_project.this.id
  username           = "stripe_app"
  password           = var.app_password
  auth_database_name = "admin"

  roles {
    role_name     = "readWrite"
    database_name = "stripe_nosql"
  }

  scopes {
    name = mongodbatlas_cluster.this.name
    type = "CLUSTER"
  }
}
