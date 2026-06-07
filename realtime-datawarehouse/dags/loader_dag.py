from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

start_date = datetime(2026, 6 ,1)

default_args = {
    "owner": "long.vk",
    "depends_on_past": False,
    "backfill": False,
    'start_date': start_date
}

with DAG(
    dag_id='dimension_batch_ingestion',
    default_args=default_args,
    description='A DAG that ingest dimension data into Apache Pinot',
    schedule='0 0 * * *',
    catchup=False
) as dag:
    start = EmptyOperator(
        task_id='start_task'
    )
    end = EmptyOperator(
        task_id='end_task'
    )

    ingest_account_dim = BashOperator(
        task_id='ingest_account_dim',
        bash_command = (
            'curl -X POST -F file=@/opt/airflow/account_dim_large_date.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=account_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22csv%22%2C%22recordReader.prop.delimiter%22%3A%22,%22%7D"'
        )
    )

    ingest_branch_dim = BashOperator(
        task_id='ingest_branch_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/branch_dim_large_date.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=branch_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22csv%22%2C%22recordReader.prop.delimiter%22%3A%22,%22%7D"'
        )
    )

    ingest_customer_dim = BashOperator(
        task_id='ingest_customer_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/customer_dim_large_date.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=customer_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22csv%22%2C%22recordReader.prop.delimiter%22%3A%22,%22%7D"'
        )
    )

    start >> [ingest_account_dim, ingest_branch_dim, ingest_customer_dim] >> end