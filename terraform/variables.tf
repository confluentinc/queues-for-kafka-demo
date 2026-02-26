# -----------------------------------------------------------------------------
# Confluent Cloud credentials
# -----------------------------------------------------------------------------

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (Cloud resource management key, not a cluster key)."
  type        = string
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret."
  type        = string
  sensitive   = true
}

# -----------------------------------------------------------------------------
# Cluster configuration
# -----------------------------------------------------------------------------

variable "cloud_provider" {
  description = "Cloud provider for the Confluent Cloud cluster (AWS, AZURE, or GCP)."
  type        = string
  default     = "AWS"

  validation {
    condition     = contains(["AWS", "AZURE", "GCP"], upper(var.cloud_provider))
    error_message = "cloud_provider must be one of: AWS, AZURE, GCP."
  }
}

variable "cloud_region" {
  description = "Cloud region for the Confluent Cloud cluster (e.g. us-east-2, eastus, us-central1)."
  type        = string
  default     = "us-east-2"
}

variable "prefix" {
  description = "Prefix for all resource display names."
  type        = string
  default     = "qfk-demo"
}
