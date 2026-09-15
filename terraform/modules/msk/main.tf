# =============================================================================
# Module msk — Kafka managé (équivalent du service `kafka` KRaft de
# docker-compose.yml + topics de scripts/create_topics.sh)
#
# Différences voulues avec le PoC (1 broker, facteur de réplication 1) :
#   - 3 brokers répartis sur 3 AZ, réplication 3, min.insync.replicas 2 :
#     la perte d'une AZ n'interrompt ni la production ni la consommation.
#   - Création automatique de topics DÉSACTIVÉE : en local elle est active
#     (KAFKA_AUTO_CREATE_TOPICS_ENABLE=true) pour que Debezium crée ses topics
#     CDC ; en prod chaque topic est déclaré explicitement (partitions,
#     rétention) pour éviter des topics fantômes créés par une faute de frappe.
#   - Authentification IAM + TLS obligatoire (plus aucun listener PLAINTEXT).
# =============================================================================

resource "aws_msk_configuration" "this" {
  name           = "${var.name}-kafka"
  kafka_versions = [var.kafka_version]

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=6
    log.retention.hours=168
    unclean.leader.election.enable=false
  PROPERTIES
}

resource "aws_cloudwatch_log_group" "broker" {
  name              = "/aws/msk/${var.name}"
  retention_in_days = 30
  kms_key_id        = var.kms_key_arn
  tags              = var.tags
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.name}-msk"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [var.security_group_id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_volume_gb
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  client_authentication {
    sasl {
      iam = true
    }
    unauthenticated = false
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = var.kms_key_arn
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.broker.name
      }
    }
  }

  # Métriques par topic/partition : nécessaires pour suivre le lag du consumer
  # group flink-fraud-scorer (le SLA "< 100 ms" en dépend directement).
  enhanced_monitoring = "PER_TOPIC_PER_PARTITION"

  tags = var.tags
}
