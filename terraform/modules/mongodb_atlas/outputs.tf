output "cluster_name" {
  description = "Nom du cluster Atlas."
  value       = mongodbatlas_cluster.this.name
}

output "private_connection_strings" {
  description = "Chaînes de connexion PrivateLink (équivalent cloud de MONGO_HOST)."
  value       = mongodbatlas_cluster.this.connection_strings
  sensitive   = true
}
