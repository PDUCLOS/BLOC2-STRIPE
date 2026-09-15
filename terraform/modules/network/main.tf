# =============================================================================
# Module network — VPC multi-AZ de la cible AWS
#
# Traduit la couche réseau décrite dans docs/ARCHITECTURE.md (cible
# industrialisée) : 3 niveaux de sous-réseaux par zone de disponibilité.
#   - public  : uniquement NAT Gateway (aucune ressource applicative exposée)
#   - app     : tâches ECS Fargate (scorer, mongo-writer, dashboard, ml-monitor), MWAA
#   - data    : RDS PostgreSQL, MSK, ElastiCache, endpoint PrivateLink Atlas
# Équivalent local : le réseau bridge par défaut de docker-compose.yml.
# =============================================================================

locals {
  az_count = length(var.azs)
  # Découpage déterministe du /16 : /20 par sous-réseau (4 094 IP chacun).
  # Index 0-2 public, 3-5 app, 6-8 data — laisse de la place pour d'autres niveaux.
  public_cidrs = [for i in range(local.az_count) : cidrsubnet(var.cidr_block, 4, i)]
  app_cidrs    = [for i in range(local.az_count) : cidrsubnet(var.cidr_block, 4, i + 3)]
  data_cidrs   = [for i in range(local.az_count) : cidrsubnet(var.cidr_block, 4, i + 6)]
  # Un NAT par AZ en prod (une panne d'AZ ne coupe pas les autres),
  # un seul NAT partagé en dev (FinOps : ~32 $/mois par NAT économisé).
  nat_count = var.single_nat_gateway ? 1 : local.az_count
}

resource "aws_vpc" "this" {
  cidr_block           = var.cidr_block
  enable_dns_support   = true
  enable_dns_hostnames = true # requis pour la résolution privée des endpoints PrivateLink

  tags = merge(var.tags, { Name = "${var.name}-vpc" })
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = merge(var.tags, { Name = "${var.name}-igw" })
}

resource "aws_subnet" "public" {
  count                   = local.az_count
  vpc_id                  = aws_vpc.this.id
  cidr_block              = local.public_cidrs[count.index]
  availability_zone       = var.azs[count.index]
  map_public_ip_on_launch = false # rien ne doit recevoir d'IP publique par défaut

  tags = merge(var.tags, { Name = "${var.name}-public-${var.azs[count.index]}", Tier = "public" })
}

resource "aws_subnet" "app" {
  count             = local.az_count
  vpc_id            = aws_vpc.this.id
  cidr_block        = local.app_cidrs[count.index]
  availability_zone = var.azs[count.index]

  tags = merge(var.tags, { Name = "${var.name}-app-${var.azs[count.index]}", Tier = "app" })
}

resource "aws_subnet" "data" {
  count             = local.az_count
  vpc_id            = aws_vpc.this.id
  cidr_block        = local.data_cidrs[count.index]
  availability_zone = var.azs[count.index]

  tags = merge(var.tags, { Name = "${var.name}-data-${var.azs[count.index]}", Tier = "data" })
}

# --- Sortie Internet des sous-réseaux privés (pull d'images, APIs AWS publiques) ---

resource "aws_eip" "nat" {
  count  = local.nat_count
  domain = "vpc"
  tags   = merge(var.tags, { Name = "${var.name}-nat-eip-${count.index}" })
}

resource "aws_nat_gateway" "this" {
  count         = local.nat_count
  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id
  tags          = merge(var.tags, { Name = "${var.name}-nat-${count.index}" })

  depends_on = [aws_internet_gateway.this]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = merge(var.tags, { Name = "${var.name}-rt-public" })
}

resource "aws_route_table_association" "public" {
  count          = local.az_count
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# Une table de routage privée par AZ : chaque AZ sort par "son" NAT
# (ou par l'unique NAT partagé si single_nat_gateway = true).
resource "aws_route_table" "private" {
  count  = local.az_count
  vpc_id = aws_vpc.this.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this[var.single_nat_gateway ? 0 : count.index].id
  }

  tags = merge(var.tags, { Name = "${var.name}-rt-private-${var.azs[count.index]}" })
}

resource "aws_route_table_association" "app" {
  count          = local.az_count
  subnet_id      = aws_subnet.app[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# Les sous-réseaux data n'ont PAS de route vers Internet : RDS, MSK et
# ElastiCache ne sortent jamais (défense en profondeur, PCI-DSS req. 1.3).
resource "aws_route_table" "data" {
  vpc_id = aws_vpc.this.id
  tags   = merge(var.tags, { Name = "${var.name}-rt-data" })
}

resource "aws_route_table_association" "data" {
  count          = local.az_count
  subnet_id      = aws_subnet.data[count.index].id
  route_table_id = aws_route_table.data.id
}

# --- Endpoints VPC : le trafic vers S3 ne transite ni par NAT ni par Internet ---

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = concat(aws_route_table.private[*].id, [aws_route_table.data.id])

  tags = merge(var.tags, { Name = "${var.name}-vpce-s3" })
}

# --- VPC Flow Logs (traçabilité réseau, PCI-DSS req. 10) ---

resource "aws_cloudwatch_log_group" "flow_logs" {
  name              = "/aws/vpc/${var.name}/flow-logs"
  retention_in_days = var.flow_logs_retention_days
  kms_key_id        = var.kms_key_arn
  tags              = var.tags
}

data "aws_iam_policy_document" "flow_logs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["vpc-flow-logs.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flow_logs" {
  name               = "${var.name}-vpc-flow-logs"
  assume_role_policy = data.aws_iam_policy_document.flow_logs_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "flow_logs_write" {
  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
    ]
    resources = ["${aws_cloudwatch_log_group.flow_logs.arn}:*"]
  }
}

resource "aws_iam_role_policy" "flow_logs" {
  name   = "write-flow-logs"
  role   = aws_iam_role.flow_logs.id
  policy = data.aws_iam_policy_document.flow_logs_write.json
}

resource "aws_flow_log" "this" {
  vpc_id          = aws_vpc.this.id
  traffic_type    = "ALL"
  iam_role_arn    = aws_iam_role.flow_logs.arn
  log_destination = aws_cloudwatch_log_group.flow_logs.arn
  tags            = var.tags
}
