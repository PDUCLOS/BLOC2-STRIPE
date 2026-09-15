terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
    mongodbatlas = {
      source = "mongodb/mongodbatlas" # éditeur MongoDB, pas hashicorp
    }
  }
}
