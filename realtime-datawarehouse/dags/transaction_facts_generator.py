from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from kafka_operator import KafkaProduceOperator

start_date = datetime(2026, 6, 1)
default_args = {
    "owner": "long.vk",
    "depends_on_past": False,
    "backfill": False
}

with DAG(
    dag_id='transaction_facts_generator',
    default_args=default_args,
    description='Transaction fact data generator into kafka',
    schedule="0 0 * * *",
    tags=['fact_data', 'transaction']
) as dag:
    start = EmptyOperator(
        task_id='start_task'
    )
    end = EmptyOperator(
        task_id='end_task'
    )

    generate_txn_data = KafkaProduceOperator(
        task_id='generate_txn_fact_data',
        kafka_broker='kafka:9092',
        kafka_topic='transaction_facts',
        num_records=100
    )

    start >> generate_txn_data >> end