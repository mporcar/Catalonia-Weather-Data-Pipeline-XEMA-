variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
}

variable "location" {
  description = "Google Cloud Location/Region for resources"
  type        = string
  default     = "europe-southwest1"
}

variable "bucket_name" {
  description = "Name for the Google Cloud Storage bucket (raw data lake)"
  type        = string
}

variable "bq_dataset_name" {
  description = "Name of the BigQuery Dataset"
  type        = string
  default     = "xema_weather"
}
