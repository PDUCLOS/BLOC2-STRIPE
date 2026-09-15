# =============================================================================
# Environnement DEV — tailles réduites, un seul NAT, pas de Multi-AZ RDS
# Objectif : valider les déploiements et la CI à moindre coût (cf. docs/FINOPS.md).
# =============================================================================

provider "aws" {
  region = var.region

  # Tags appliqués à TOUTES les ressources AWS : indispensables à l'allocation
  # des coûts par projet/environnement dans Cost Explorer et au budget.
  default_tags {
    tags = local.tags
  }
}

# Les clés Atlas sont lues depuis MONGODB_ATLAS_PUBLIC_KEY / MONGODB_ATLAS_PRIVATE_KEY
# (jamais dans le code ni dans terraform.tfvars).
provider "mongodbatlas" {}

locals {
  tags = {
    Project     = "stripe-polyglot"
    Environment = "dev"
    ManagedBy   = "terraform"
    Owner       = "patrice-duclos"
    DataClass   = "pci-test-data" # aucune vraie donnée de carte en dev
  }
}

module "stack" {
  source = "../../stack"

  environment = "dev"
  region      = var.region
  azs         = ["${var.region}a", "${var.region}b", "${var.region}c"]
  vpc_cidr    = "10.20.0.0/16"

  single_nat_gateway = true

  rds_instance_class     = "db.t4g.medium"
  rds_multi_az           = false
  rds_read_replica_count = 0

  msk_broker_count         = 3 # MSK impose un multiple du nombre d'AZ
  msk_broker_instance_type = "kafka.t3.small"

  redis_node_type = "cache.t4g.small"
  redis_num_nodes = 1

  atlas_org_id        = var.atlas_org_id
  atlas_instance_size = "M10"

  scorer_desired_count = 1
  image_tag            = var.image_tag

  alert_email        = var.alert_email
  monthly_budget_usd = 900

  tags = local.tags
}
