# =============================================================================
# Service accounts, role bindings, and API keys
#
# One service account ("app-manager") with CloudClusterAdmin is used for
# both the standard demo and the KEDA scaler. CloudClusterAdmin provides
# the permissions needed to:
#   - Produce and consume messages
#   - Describe and list share group offsets (required by the KEDA scaler)
#   - Manage topics
# =============================================================================

# -----------------------------------------------------------------------------
# Service account
# -----------------------------------------------------------------------------

resource "confluent_service_account" "app-manager" {
  display_name = "${var.prefix}-app-manager-${random_id.suffix.hex}"
  description  = "Service account for the Queues for Kafka demo"
}

# -----------------------------------------------------------------------------
# Role binding — CloudClusterAdmin on the cluster
# -----------------------------------------------------------------------------

resource "confluent_role_binding" "app-manager-cluster-admin" {
  principal   = "User:${confluent_service_account.app-manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.demo.rbac_crn
}

# -----------------------------------------------------------------------------
# Kafka API key
#
# Used by producers, consumers, and the KEDA external scaler.
# -----------------------------------------------------------------------------

resource "confluent_api_key" "app-manager" {
  display_name = "${var.prefix}-app-manager-key-${random_id.suffix.hex}"
  description  = "Kafka API key for the Queues for Kafka demo"

  disable_wait_for_ready = true

  owner {
    id          = confluent_service_account.app-manager.id
    api_version = confluent_service_account.app-manager.api_version
    kind        = confluent_service_account.app-manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.demo.id
    api_version = confluent_kafka_cluster.demo.api_version
    kind        = confluent_kafka_cluster.demo.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_role_binding.app-manager-cluster-admin
  ]
}
