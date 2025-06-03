resource "google_bigquery_dataset" "staging_dataset" {
  project                     = var.project_id
  dataset_id                  = "staging_dataset"
  friendly_name               = "Staging dataset"
  description                 = "Staging dataset for ELT"
  location                    = var.region
  default_table_expiration_ms = 3600000

  labels = {
    env = "elt"
    created_by = "terraform"
    owner = "rio"
  }
}

resource "google_bigquery_dataset" "transformed_dataset" {
  project                     = var.project_id
  dataset_id                  = "transformed_dataset"
  friendly_name               = "Transformed dataset"
  description                 = "Transformed dataset for ELT"
  location                    = var.region
  default_table_expiration_ms = 3600000

  labels = {
    env = "elt"
    created_by = "terraform"
    owner = "rio"
  }
}

resource "google_storage_bucket" "ingestion_bucket" {
  project       = var.project_id 
  name          = "sandbox-402413-ingestion-bucket"
  location      = var.region
  uniform_bucket_level_access = true
}
