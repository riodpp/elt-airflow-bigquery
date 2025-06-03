import logging
import os
from airflow.sdk import DAG, Asset
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.api_functions import get_new_sales_from_internal_api
from pprint import pprint
from airflow.models.param import Param
import random
from airflow.sdk import Asset, Metadata

t_log = logging.getLogger("airflow.task")

_GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

gcs_asset = Asset(f"gs://{_GCS_BUCKET_NAME}/data-*")

with DAG(
    dag_id='load_to_gcs',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    
) as dag:
    def get_new_sales_from_api(**context):
        # pprint(context)
        num_sales = 100
        date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        sales_df, users_df, cheeses_df, utm_df = get_new_sales_from_internal_api(
            num_sales, date
        )

        t_log.info(f"Fetching {num_sales} new sales from the internal API.")
        t_log.info(f"Head of the new sales data: {sales_df.head()}")
        t_log.info(f"Head of the new users data: {users_df.head()}")
        t_log.info(f"Head of the new cheese data: {cheeses_df.head()}")
        t_log.info(f"Head of the new utm data: {utm_df.head()}")

        return [
            {"name": "sales", "data": sales_df},
            {"name": "users", "data": users_df},
            {"name": "cheeses", "data": cheeses_df},
            {"name": "utms", "data": utm_df},
        ]

    create_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_bucket",
        bucket_name=_GCS_BUCKET_NAME,
        storage_class="STANDARD",
        location="US",
        gcp_conn_id="google_cloud_default"
    )

    get_new_sales_from_api_obj = PythonOperator(
        task_id='get_new_sales_from_api',
        python_callable=get_new_sales_from_api
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src="/usr/local/airflow/*.csv",
        dst="data-{{ ds }}/",
        bucket=_GCS_BUCKET_NAME
    )

    create_bucket >> get_new_sales_from_api_obj >> upload_file