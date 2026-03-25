variable "project" {
  description = "Your GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region for your resources"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "The GCP location for BigQuery dataset"
  type        = string
  default     = "us-central1"
}

variable "credentials" {
  description = "Path to your GCP service account JSON key file"
  type        = string
}
# Note: I made the credentials an environment variable named TF_VAR_credentials 
# to avoid hardcoding sensitive information in terraform.tfvars,
# so it doesn't appear in the terraform.tfvars file, but it can still be used in the Terraform configuration.

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket for raw data (data lake)"
  type        = string
}

variable "bq_dataset_name" {
  description = "Name of the BigQuery dataset"
  type        = string
}
