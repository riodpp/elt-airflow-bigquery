import os
from typing import cast

from airflow.sdk import DAG, Asset
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import timedelta

from pendulum import datetime, duration

_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
_DATASET_NAME = "staging_dataset"

with DAG(
    dag_id='load_to_bigquery',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    ets_load_to_gcs= ExternalTaskSensor(
        task_id="ets_load_to_gcs",
        external_dag_id="load_to_gcs",
        external_task_id="upload_file",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_delta=timedelta(hours=1),
        timeout=60 * 30,
        poke_interval=60,
        mode='poke',
        check_existence=True,
    )

    load_utms = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_utms",
        bucket=_BUCKET_NAME,
        source_objects=["data-{{ ds }}/utms.csv"],
        source_format="CSV",
        destination_project_dataset_table=f"{_DATASET_NAME}.utms",
        schema_fields=[
            {"name": "utm_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "utm_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "utm_medium", "type": "STRING", "mode": "NULLABLE"},
            {"name": "utm_campaign", "type": "STRING", "mode": "NULLABLE"},
            {"name": "utm_term", "type": "STRING", "mode": "NULLABLE"},
            {"name": "utm_content", "type": "STRING", "mode": "NULLABLE"},
            {"name": "updated_at", "type": "DATE", "mode": "REQUIRED"}
        ],
        write_disposition="WRITE_TRUNCATE",
        outlets=[Asset(f"staging_dataset.utm")],
    )

    load_users = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_users",
        bucket=_BUCKET_NAME,
        source_objects=["data-{{ ds }}/users.csv"],
        destination_project_dataset_table=f"{_DATASET_NAME}.users",
        schema_fields=[
            {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "user_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "date_of_birth", "type": "DATE", "mode": "NULLABLE"},
            {"name": "sign_up_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "DATE", "mode": "REQUIRED"}
        ],
        write_disposition="WRITE_TRUNCATE",
        outlets=[Asset(f"staging_dataset.users")],
    )

    load_cheeses = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_cheeses",
        bucket=_BUCKET_NAME,
        source_objects=["data-{{ ds }}/cheeses.csv"],
        destination_project_dataset_table=f"{_DATASET_NAME}.cheeses",
        schema_fields=[
            {"name": "cheese_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "cheese_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "cheese_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "price", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "DATE", "mode": "REQUIRED"}
        ],
        write_disposition="WRITE_TRUNCATE",
        outlets=[Asset(f"staging_dataset.cheeses")],
    )

    load_sales = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_sales",
        bucket=_BUCKET_NAME,
        source_objects=["data-{{ ds }}/sales.csv"],
        destination_project_dataset_table=f"{_DATASET_NAME}.sales",
        schema_fields=[
            {"name": "sale_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "cheese_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "utm_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "quantity", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "sale_date", "type": "TIMESTAMP", "mode": "REQUIRED"}
        ],
        write_disposition="WRITE_TRUNCATE",
        outlets=[Asset(f"staging_dataset.sales")],
    )

    ets_load_to_gcs >> [load_utms, load_users, load_cheeses, load_sales]