terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Service Account for the Data Pipeline
resource "google_service_account" "pipeline_sa" {
  account_id   = "xema-pipeline-sa"
  display_name = "XEMA Pipeline Service Account"
}

# IAM Role: Storage Admin
resource "google_project_iam_member" "sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# IAM Role: BigQuery Admin
resource "google_project_iam_member" "sa_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Generates keys for local testing, though Workload Identity is preferred in production.
resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}

# GCS Bucket (Data Lake)
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 365 # Retain raw data for 1 year
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "weather_dataset" {
  dataset_id                  = var.bq_dataset_name
  location                    = var.region
  description                 = "Dataset for XEMA weather data pipeline"
  delete_contents_on_destroy  = true
}

# Output the Service Account Key (Handle with care in production!)
output "service_account_private_key" {
  value     = google_service_account_key.pipeline_sa_key.private_key
  sensitive = true
}
