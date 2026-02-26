terraform {
  required_version = ">= 1.3"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = ">= 2.20.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}
