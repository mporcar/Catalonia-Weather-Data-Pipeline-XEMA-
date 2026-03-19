terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

# Configure the Google Cloud provider
provider "google" {
  project = var.project_id
  region  = var.location
}

# ------------------------------------------------------------------------------
# Google Cloud Storage (Data Lake)
# ------------------------------------------------------------------------------

# GCS Bucket to store raw data
resource "google_storage_bucket" "data_lake" {
  # Ensures globally unique, lowercase name without spaces
  name          = var.bucket_name
  location      = var.location
  force_destroy = true # Allows deletion of bucket even if it contains objects

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 365 # Retain data for 1 year
    }
    action {
      type = "Delete"
    }
  }
}

# ------------------------------------------------------------------------------
# BigQuery Dataset (Data Warehouse)
# ------------------------------------------------------------------------------

# BigQuery Dataset where dbt will create models and transformations
resource "google_bigquery_dataset" "weather_dataset" {
  dataset_id                 = var.bq_dataset_name
  location                   = var.location
  description                = "Dataset for XEMA weather data pipeline"
  delete_contents_on_destroy = true # Allows deletion of dataset even if it contains tables
}
