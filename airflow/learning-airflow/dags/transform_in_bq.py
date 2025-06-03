from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateTableOperator, BigQueryCreateEmptyDatasetOperator
from airflow import DAG
from airflow.datasets import Dataset
from pendulum import datetime, duration
import datetime as dt
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import os


project_id = os.getenv("PROJECT_ID")
transformed_dataset = os.getenv("TRANSFORMED_DATASET")
staging_dataset = os.getenv("STAGING_DATASET")

def _generate_merge_query(**context):
    """Returns the merge query with the execution date."""
    ds = context["ds"]
    query = f"""
        MERGE INTO `{project_id}.{transformed_dataset}.sales_transformed` AS target
        USING (
            SELECT s.*, u.user_name, b.cheese_name, b.cheese_type, b.price,
                   uparams.utm_source, uparams.utm_medium, uparams.utm_campaign,
                   uparams.utm_term, uparams.utm_content
            FROM `{project_id}.{staging_dataset}.sales` s
            JOIN `{project_id}.{staging_dataset}.users` u ON s.user_id = u.user_id
            JOIN `{project_id}.{staging_dataset}.cheeses` b ON s.cheese_id = b.cheese_id
            JOIN `{project_id}.{staging_dataset}.utms` uparams ON s.utm_id = uparams.utm_id
            WHERE DATE(s.sale_date) = DATE('{ds}')
        ) AS source
        ON target.sale_id = source.sale_id

        WHEN MATCHED THEN UPDATE SET
            user_id = source.user_id,
            user_name = source.user_name,
            cheese_name = source.cheese_name,
            cheese_type = source.cheese_type,
            quantity = source.quantity,
            sale_date = source.sale_date,
            utm_source = source.utm_source,
            utm_medium = source.utm_medium,
            utm_campaign = source.utm_campaign,
            utm_term = source.utm_term,
            utm_content = source.utm_content,
            total_revenue = source.price * source.quantity

        WHEN NOT MATCHED THEN INSERT (
            sale_id,
            user_id,
            user_name,
            cheese_name,
            cheese_type,
            quantity,
            sale_date,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_term,
            utm_content,
            total_revenue
        )
        VALUES (
            source.sale_id,
            source.user_id,
            source.user_name,
            source.cheese_name,
            source.cheese_type,
            source.quantity,
            source.sale_date,
            source.utm_source,
            source.utm_medium,
            source.utm_campaign,
            source.utm_term,
            source.utm_content,
            source.price * source.quantity
        )
    """

    context['ti'].xcom_push(key='merge_query', value=query)
    return query


with DAG(
    dag_id="bq_merge_sales_dag",
    start_date=datetime(2023, 1, 1),
    schedule=[
        Dataset(f"{transformed_dataset}.utm"),
        Dataset(f"{transformed_dataset}.users"),
        Dataset(f"{transformed_dataset}.cheeses"),
        Dataset(f"{transformed_dataset}.sales")
    ],
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=f"{transformed_dataset}",
        project_id=f"{project_id}",
        exists_ok=True,
        location="us-central1",
    )

    create_table_if_needed = BigQueryCreateTableOperator(
        task_id="create_table_if_needed",
        project_id=f"{project_id}",
        dataset_id=f"{transformed_dataset}",
        table_id="sales_transformed",
        table_resource={
            "schema": {
                "fields": [
                    {"name": "sale_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "user_name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "cheese_name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "cheese_type", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "sale_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "utm_source", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "utm_medium", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "utm_campaign", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "utm_term", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "utm_content", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "total_revenue", "type": "FLOAT", "mode": "NULLABLE"},
                ],
            },
            "timePartitioning": {
                "type": "DAY",
                "field": "sale_date",
            },
        },
        if_exists="ignore",
        location="us-central1",
    )

    # Generate the merge query
    generate_query = PythonOperator(
        task_id="generate_query",
        python_callable=_generate_merge_query
    )

    # Execute the merge operation
    merge_sales = BigQueryInsertJobOperator(
        task_id="merge_sales",
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='generate_query', key='merge_query') }}",
                "useLegacySql": False,
            }
        },
        location="us-central1",
    )

    # Set task dependencies
    create_dataset >> create_table_if_needed >> generate_query >> merge_sales