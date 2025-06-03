
# Terraform Infrastructure for ELT Pipeline

This Terraform configuration provisions the Google Cloud Platform infrastructure required for the ELT pipeline using Apache Airflow, Google Cloud Storage, and BigQuery.

## Infrastructure Components

The Terraform configuration creates the following GCP resources:

### 1. BigQuery Dataset
- **Resource**: `google_bigquery_dataset.staging_dataset`
- **Dataset ID**: `staging_dataset`
- **Purpose**: Staging area for raw data before transformation
- **Location**: Configurable via `region` variable
- **Features**:
  - Table expiration: 1 hour (3600000 ms)
  - Labels for resource management
  - Friendly name and description

### 2. Google Cloud Storage Bucket
- **Resource**: `google_storage_bucket.ingestion_bucket`
- **Bucket Name**: `ingestion-bucket-purwadika`
- **Purpose**: Storage for raw data files from API extractions
- **Features**:
  - Uniform bucket-level access enabled
  - Regional storage for cost optimization
  - Integrated with Airflow DAGs for data pipeline

## Project Structure
```
terraform/
├── main.tf                                 # Main infrastructure resources
├── variables.tf                            # Variable definitions
├── terraform.tf                            # Terraform and provider configuration
├── providers/
└── terraform.tfstate
airflow/
└── learning-airflow/
    ├── .astro/
    ├── dags/
        ├── utils/                          # DAG for data ingestion
            └── api_extraction.py           # Script for data generation
        ├── load_to_gcs.py                  # DAG for load data to GCS
        ├── transform_data.py               # DAG for data transformation
        ├── load_to_bq.py                   # DAG for load data to BigQuery
        └── .airflowignore
    ├── include/
    ├── plugins/
    ├── test/
    ├── env_copy                            # Environment variables for Airflow
    ├── .gitignore
    ├── Dockerfile
    ├── packages.txt
    ├── README.md
    └── requirements.txt

```


## Prerequisites

1. **Google Cloud Platform**:
   - Active GCP project
   - Billing enabled
   - Required APIs enabled:
     - BigQuery API
     - Cloud Storage API
     - Cloud Resource Manager API

2. **Authentication**:
   - Service account with appropriate permissions:
     - BigQuery Admin
     - Storage Admin
     - Project Editor (or specific resource permissions)
   - Create the service account key and populate them in the airflow .env
   - Application Default Credentials configured:
     ```bash
     gcloud auth application-default login
     ```

3. **Terraform**:
   - Terraform >= 1.0
   - Google Cloud Provider >= 4.0

## Setup Resource using Terraform

### 1. Initialize Terraform
```bash
cd terraform
terraform init
```

### 2. Configure Variables
Create a `variables.tfvars` file with the required variables:
```hcl
project_id = "your-gcp-project-id"
region     = "your-gcp-region"
```

### 3. Plan Terraform
```bash
terraform plan -var-file="variables.tfvars"
```

### 4. Apply Terraform
```bash
terraform apply -var-file="variables.tfvars" --auto-apply
```

## Airflow Setup
### 1. Copy env file
```bash
cp airflow/learning-airflow/env_copy airflow/learning-airflow/.env
```

### 2. Running airflow using astro
```bash
cd airflow/learning-airflow
astro dev start
```


