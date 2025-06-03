terraform {
        required_version = "~>1.10.0"
        backend "gcs" {
            bucket = "sandbox-402413-elt-airflow-bq-tf-state"
            prefix = "state"
        }
        required_providers {
            google = {
            source = "hashicorp/google"
            version = "~>6.21.0"
        }
    }
}