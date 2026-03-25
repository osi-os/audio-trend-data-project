# ---------------------------------------------------------
# TERRAFORM CONFIGURATION
# This block tells Terraform which provider plugins it needs.
# Think of it like a "requirements.txt" — it says
# "I need the Google Cloud plugin, version 6.x"
# ---------------------------------------------------------
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

# ---------------------------------------------------------
# PROVIDER CONFIGURATION
# This is the "connection" to GCP. It tells Terraform:
#   - Which project to manage
#   - Which region to default to
#   - How to authenticate (your service account key)
# Every resource below will inherit these settings.
# ---------------------------------------------------------
provider "google" {
  project     = var.project
  region      = var.region
  credentials = var.credentials
}

# ---------------------------------------------------------
# RESOURCE 1: Google Cloud Storage Bucket (Data Lake)
# 
# This is where your raw data files land before they get
# loaded into BigQuery. Think of it as the "landing zone."
#
# Key properties:
#   - name: must be globally unique across ALL of GCP
#   - location: where the data physically lives
#   - force_destroy: if true, Terraform can delete the bucket
#     even if it still has files in it (useful for cleanup)
#   - lifecycle_rule: automatically deletes files older than
#     30 days to keep storage costs low
# ---------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  # Optional: auto-cleanup old raw files after 30 days
  lifecycle_rule {
    condition {
      age = 30  # days
    }
    action {
      type = "Delete"
    }
  }
}

# ---------------------------------------------------------
# RESOURCE 2: BigQuery Dataset (Data Warehouse)
#
# A dataset in BigQuery is like a "schema" or "database" —
# it's a container that holds your tables. Your Bruin SQL
# assets will create tables inside this dataset.
#
# Key properties:
#   - dataset_id: the name you'll reference in SQL
#     (e.g. SELECT * FROM audio_trends.my_table)
#   - location: must match or be compatible with your
#     GCS bucket location for loading data
#   - delete_contents_on_destroy: lets Terraform clean up
#     tables when you run terraform destroy
# ---------------------------------------------------------
resource "google_bigquery_dataset" "audio_trends" {
  dataset_id                = var.bq_dataset_name
  location                  = var.location
  delete_contents_on_destroy = true
}
