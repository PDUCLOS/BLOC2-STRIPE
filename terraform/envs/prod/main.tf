# =============================================================================
# Environnement PROD — cible industrialisée décrite dans la présentation
# (slide "Du PoC local à la cible cloud") : Multi-AZ partout, un NAT par AZ,
# réplica de lecture, 3 brokers MSK sur 3 AZ, Redis avec bascule automatique.
# =============================================================================

provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}

provider "mongodbatlas" {}

locals {
  tags = {
    Project     = "stripe-polyglot"
    Environment = "prod"
    ManagedBy   = "terraform"
    Owner       = "patrice-duclos"
    DataClass   = "pci-cardholder" # périmètre PCI-DSS : revues d'accès trimestrielles
  }
}

module "stack" {
  source = "../../stack"

  environment = "prod"
  region      = var.region
  azs         = ["${var.region}a", "${var.region}b", "${var.region}c"]
  vpc_cidr    = "10.30.0.0/16" # plage distincte de dev : peering possible sans conflit

  single_nat_gateway = false

  rds_instance_class     = "db.r7g.large"
  rds_multi_az           = true
  rds_read_replica_count = 1

  msk_broker_count         = 3
  msk_broker_instance_type = "kafka.m7g.large"

  redis_node_type = "cache.r7g.large"
  redis_num_nodes = 2

  atlas_org_id        = var.atlas_org_id
  atlas_instance_size = "M30"

  scorer_desired_count = 3
  image_tag            = var.image_tag

  alert_email        = var.alert_email
  monthly_budget_usd = 4000

  tags = local.tags
}
