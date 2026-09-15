# Même bucket de state que dev, clé distincte : un `apply` en dev ne peut
# jamais modifier le state de prod.
terraform {
  backend "s3" {
    bucket       = "stripe-polyglot-tfstate"
    key          = "prod/terraform.tfstate"
    region       = "eu-west-1"
    encrypt      = true
    use_lockfile = true
  }
}
