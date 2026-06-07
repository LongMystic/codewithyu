from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from pinot_table_operator import PinotTableSubmitOperator

start_date = datetime(2026, 6, 1)
default_args = {
    "owner": "long.vk",
    "depends_on_past": False,
    "backfill": False,
    'start_date': start_date
}

with DAG(
    dag_id='table_dag',
    default_args=default_args,
    description='A Dag to submit all tables in a folder to Apache Pinot',
    schedule="0 0 * * *",
    tags=['table']
) as dag:
    start = EmptyOperator(
        task_id='start_task'
    )
    end = EmptyOperator(
        task_id='end_task'
    )

    submit_table = PinotTableSubmitOperator(
        task_id='submit_pinot_tables',
        folder_path='/opt/airflow/dags/tables',
        pinot_url='http://pinot-controller:9000/tables'
    )

    start >> submit_table >> end