
# Summary

This project is inspired by the project from https://www.astronomer.io/docs/learn/reference-architecture-elt-bigquery-dbt. In this project I utilize Astronomer and Airflow 3 to create DAG. I use Terraform to provision GCP resources.

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
   - Terraform >= 1.10
   - Google Cloud Provider >= 6.21.0

4. **Astro**:
   - Astro CLI installed:
     ```bash
     brew install astro@1.34.1
     ```
   - Installation for windows:
     https://www.astronomer.io/docs/astro/cli/install-cli?tab=windowswithwinget#install-the-astro-cli

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
Create .env file and put your credential in it.
```bash
cp airflow/learning-airflow/env_copy airflow/learning-airflow/.env
```

### 2. Running airflow using astro
```bash
cd airflow/learning-airflow
astro dev start
```

## What I learned
- How to use Terraform to provision GCP resources
- How to use Airflow to create DAGs with Astronomer
- Integrating Airflow with GCS and BigQuery
- Utilize Asset aware scheduler
- Utilize External Task Sensor
- Utilize BigQuery Operators and Hooks
- Generate syntehtic data using Python with Faker
- Implement Incremental transformation using MERGE statement.

## Possible Development
- Add more DAGs for data transformation and loading
- Implement more data transformations
- Implement more data loading
- Integrate with DBT
- Integrate with more data resources


