import random
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd

start_date = datetime(2026, 6, 1)
default_args = {
    "owner": "long.vk",
    "depends_on_past": False,
    "backfill": False
}

num_rows = 50
output_file = './account_dim_large_date.csv'

account_ids = []
account_types = []
customer_ids = []
opening_dates = []
statuses = []
balances = []

def generate_random_data(row_num):
    account_id = f'A{row_num:05d}'
    account_type = random.choice(['SAVING', 'CHECKING'])
    status = random.choice(['ACTIVE', 'INACTIVE'])
    customer_id = f'C{random.randint(1, 1000):05d}'
    balance = round(random.uniform(100.00, 10000.00), 2)

    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0, 365))
    opening_date_millis = int(random_date.timestamp() * 1000)

    return account_id, account_type, customer_id, opening_date_millis, status, balance


def generate_account_dim_data():
    row_num = 1
    while row_num <= num_rows:
        account_id, account_type, customer_id, opening_date_millis, status, balance = \
            generate_random_data(row_num)
        account_ids.append(account_id)
        account_types.append(account_type)
        customer_ids.append(customer_id)
        opening_dates.append(opening_date_millis)
        statuses.append(status)
        balances.append(balance)
        row_num += 1

    df = pd.DataFrame({
        'account_id': account_ids,
        'account_type': account_types,
        'customer_id': customer_ids,
        'opening_date': opening_dates,
        'status': statuses,
        'balance': balances
    })

    df.to_csv(output_file, index=False)

    print(f'CSV file {output_file} with {num_rows} rows has been generated successfully!')

with DAG(
    'account_dim_generator',
    default_args=default_args,
    description='Generate large account dimension data in a CSV file',
    schedule="0 0 * * *",
    start_date=start_date,
    tags=['schema', 'account']
) as dag:

    start = EmptyOperator(
        task_id = 'start_task'
    )
    end = EmptyOperator(
        task_id = 'end_task'
    )

    generate_account_dimension_data = PythonOperator(
        task_id = 'generate_account_dimension_data',
        python_callable=generate_account_dim_data
    )

    start >> generate_account_dimension_data >> end