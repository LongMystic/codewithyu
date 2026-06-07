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
output_file = './customer_dim_large_date.csv'

customer_ids = []
first_names = []
last_names = []
emails = []
phone_numbers = []
registration_dates = []

def generate_random_data(row_num):
    customer_id = f'C{row_num:05d}'
    first_name = f'FirstName_{row_num}'
    last_name = f'LastName_{row_num}'
    email = f'customer_{row_num}@example.com'
    phone_number = f"+1-800-{random.randint(1000000,9999999)}"

    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0, 365))
    opening_date_millis = int(random_date.timestamp() * 1000)

    return customer_id, first_name, last_name, email, phone_number, opening_date_millis


def generate_customer_dim_data():
    row_num = 1
    while row_num <= num_rows:
        customer_id, first_name, last_name, email, phone_number, registration_date = \
            generate_random_data(row_num)
        customer_ids.append(customer_id)
        first_names.append(first_name)
        last_names.append(last_name)
        emails.append(email)
        phone_numbers.append(phone_number)
        registration_dates.append(registration_date)
        row_num += 1

    df = pd.DataFrame({
        'customer_id': customer_ids,
        'first_name': first_names,
        'last_name': last_names,
        'email': emails,
        'phone_number': phone_numbers,
        'registration_date': registration_dates
    })

    df.to_csv(output_file, index=False)

    print(f'CSV file {output_file} with {num_rows} rows has been generated successfully!')


with DAG(
    'customer_dim_generator',
    default_args=default_args,
    description='Generate large customer dimension data in a CSV file',
    schedule="0 0 * * *",
    start_date=start_date,
    tags=['schema', 'customer']
) as dag:

    start = EmptyOperator(
        task_id = 'start_task'
    )
    end = EmptyOperator(
        task_id = 'end_task'
    )

    generate_customer_dimension_data = PythonOperator(
        task_id = 'generate_customer_dimension_data',
        python_callable=generate_customer_dim_data
    )

    start >> generate_customer_dimension_data >> end