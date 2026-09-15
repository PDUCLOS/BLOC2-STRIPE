# State distant chiffré dans S3, verrouillage natif par fichier .tflock
# (Terraform >= 1.10, plus besoin de table DynamoDB). Le bucket est créé une
# seule fois par terraform/bootstrap. Pour un simple `terraform validate`
# sans compte AWS : `terraform init -backend=false`.
terraform {
  backend "s3" {
    bucket       = "stripe-polyglot-tfstate"
    key          = "dev/terraform.tfstate"
    region       = "eu-west-1"
    encrypt      = true
    use_lockfile = true
  }
}
