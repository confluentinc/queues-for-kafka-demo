# =============================================================================
# Outputs
#
# After `terraform apply`, use these values to configure the demo.
# =============================================================================

# -----------------------------------------------------------------------------
# Cluster details
# -----------------------------------------------------------------------------

output "environment_id" {
  description = "Confluent Cloud environment ID"
  value       = confluent_environment.demo.id
}

output "cluster_id" {
  description = "Confluent Cloud cluster ID"
  value       = confluent_kafka_cluster.demo.id
}

output "bootstrap_servers" {
  description = "Kafka bootstrap servers endpoint"
  value       = replace(confluent_kafka_cluster.demo.bootstrap_endpoint, "SASL_SSL://", "")
}

# -----------------------------------------------------------------------------
# API credentials
# -----------------------------------------------------------------------------

output "kafka_api_key" {
  description = "Kafka API key"
  value       = confluent_api_key.app-manager.id
  sensitive   = true
}

output "kafka_api_secret" {
  description = "Kafka API secret"
  value       = confluent_api_key.app-manager.secret
  sensitive   = true
}

# -----------------------------------------------------------------------------
# Ready-to-use env vars for the standard demo (KProducer, QConsumer, Dashboard)
# Usage: eval "$(terraform output -raw demo_env)"
# -----------------------------------------------------------------------------

output "demo_env" {
  description = "Shell export commands for the standard demo"
  value       = <<-EOT
    export BOOTSTRAP_SERVERS="${replace(confluent_kafka_cluster.demo.bootstrap_endpoint, "SASL_SSL://", "")}"
    export CONFLUENT_API_KEY="${confluent_api_key.app-manager.id}"
    export CONFLUENT_API_SECRET="${confluent_api_key.app-manager.secret}"
  EOT
  sensitive   = true
}

# -----------------------------------------------------------------------------
# Ready-to-use env vars for the KEDA scaler
# Usage: eval "$(terraform output -raw keda_env)"
# -----------------------------------------------------------------------------

output "keda_env" {
  description = "Shell export commands for the KEDA scaler"
  value       = <<-EOT
    export KAFKA_BOOTSTRAP_SERVERS="${replace(confluent_kafka_cluster.demo.bootstrap_endpoint, "SASL_SSL://", "")}"
    export KAFKA_API_KEY="${confluent_api_key.app-manager.id}"
    export KAFKA_API_SECRET="${confluent_api_key.app-manager.secret}"
    export QFK_SHARE_GROUP_ID="chefs-share-group"
    export QFK_TOPIC_NAME="orders-queue"
    export TARGET_LAG_PER_CONSUMER="5"
  EOT
  sensitive   = true
}

# -----------------------------------------------------------------------------
# Values for Kubernetes manifests (keda-scaler/k8s/)
# Usage: terraform output k8s_values
# -----------------------------------------------------------------------------

output "k8s_values" {
  description = "Values to substitute into the KEDA Kubernetes manifests"
  value       = <<-EOT

    Substitute these into keda-scaler/k8s/ manifests:

    ccloud-admin-secret.yaml:
      KAFKA_API_KEY:    ${confluent_api_key.app-manager.id}
      KAFKA_API_SECRET: (run: terraform output -raw kafka_api_secret)

    qfk-configmap.yaml:
      KAFKA_BOOTSTRAP_SERVERS: ${replace(confluent_kafka_cluster.demo.bootstrap_endpoint, "SASL_SSL://", "")}

  EOT
  sensitive   = true
}
