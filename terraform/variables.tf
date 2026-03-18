variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud Region"
  type        = string
  default     = "europe-southwest1" # Madrid region, suitable for Spain data
}

variable "bucket_name" {
  description = "Name of the GCS Bucket to store raw data"
  type        = string
}

variable "bq_dataset_name" {
  description = "Name of the BigQuery Dataset"
  type        = string
  default     = "xema_weather"
}
