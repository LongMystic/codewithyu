from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from pinot_schema_operator import PinotSchemaSubmitOperator

start_date = datetime(2026, 6, 1)
default_args = {
    "owner": "long.vk",
    "depends_on_past": False,
    "backfill": False,
    'start_date': start_date
}

with DAG(
    dag_id='schema_dag',
    default_args=default_args,
    description='A Dag to submit all schema in a folder to Apache Pinot',
    schedule="0 0 * * *",
    tags=['schema']
) as dag:
    start = EmptyOperator(
        task_id='start_task'
    )
    end = EmptyOperator(
        task_id='end_task'
    )

    submit_schema = PinotSchemaSubmitOperator(
        task_id='submit_pinot_schemas',
        folder_path='/opt/airflow/dags/schemas',
        pinot_url='http://pinot-controller:9000/schemas'
    )

    start >> submit_schema >> end