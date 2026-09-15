output "vpc_id" {
  description = "Identifiant du VPC."
  value       = aws_vpc.this.id
}

output "vpc_cidr_block" {
  description = "CIDR du VPC (utilisé dans les règles de security groups)."
  value       = aws_vpc.this.cidr_block
}

output "public_subnet_ids" {
  description = "Sous-réseaux publics (NAT uniquement)."
  value       = aws_subnet.public[*].id
}

output "app_subnet_ids" {
  description = "Sous-réseaux privés applicatifs (ECS Fargate, MWAA)."
  value       = aws_subnet.app[*].id
}

output "data_subnet_ids" {
  description = "Sous-réseaux privés data, sans route Internet (RDS, MSK, ElastiCache, PrivateLink)."
  value       = aws_subnet.data[*].id
}
