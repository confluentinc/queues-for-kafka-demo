# =============================================================================
# Confluent Cloud Dedicated cluster for Queues for Kafka demo
#
# Creates a QfK-enabled Dedicated cluster with a public endpoint.
# Dedicated clusters support Queues for Kafka (share groups).
# =============================================================================

resource "random_id" "suffix" {
  byte_length = 4
}

# -----------------------------------------------------------------------------
# Environment
# -----------------------------------------------------------------------------

resource "confluent_environment" "demo" {
  display_name = "${var.prefix}-${random_id.suffix.hex}"

  stream_governance {
    package = "ADVANCED"
  }
}

# -----------------------------------------------------------------------------
# Dedicated Kafka cluster
#
# Dedicated clusters provide Queues for Kafka (KIP-932) support.
# Without a confluent_network resource, Dedicated clusters get a public endpoint.
# -----------------------------------------------------------------------------

resource "confluent_kafka_cluster" "demo" {
  display_name = "${var.prefix}-${random_id.suffix.hex}"
  availability = "SINGLE_ZONE"
  cloud        = upper(var.cloud_provider)
  region       = var.cloud_region

  dedicated {
    cku = 1
  }

  environment {
    id = confluent_environment.demo.id
  }
}

# -----------------------------------------------------------------------------
# Kafka topic — orders-queue
#
# Single-partition topic used by the demo. Share group consumers (chefs) and
# a standard consumer group (inventory) both read from this topic.
# -----------------------------------------------------------------------------

resource "confluent_kafka_topic" "orders" {
  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  topic_name       = "orders-queue"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.app-manager.id
    secret = confluent_api_key.app-manager.secret
  }

  depends_on = [
    confluent_role_binding.app-manager-cluster-admin
  ]
}
