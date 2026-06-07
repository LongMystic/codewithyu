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
output_file = './branch_dim_large_date.csv'

# list of sample UK cities and regions for realistic data generation
cities = ["London", "Manchester", "Birmingham"]
regions = ["London", "Scotland", "Greater Manchester"]
postcodes = ["EC1A 1BB", "M1 1AE", "EM1 1AA"]


branch_ids = []
branch_names = []
branch_addresses = []
citys = []
states = []
zipcodes = []
opening_dates = []


def generate_random_data(row_num):
    branch_id = f'B{row_num:05d}'
    branch_name = f"Branch {row_num}"
    branch_address = f"{random.randint(1, 999)} {random.choice(regions)}, {random.choice(cities)}"
    city = random.choice(cities)
    state = random.choice(regions)
    zipcode = random.choice(postcodes)

    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0, 365))
    opening_date_millis = int(random_date.timestamp() * 1000)

    return branch_id, branch_name, branch_address, city, state, zipcode, opening_date_millis


def generate_branch_dim_data():
    row_num = 1
    while row_num <= num_rows:
        branch_id, branch_name, branch_address, city, state, zipcode, opening_date_millis = \
            generate_random_data(row_num)
        branch_ids.append(branch_id)
        branch_names.append(branch_name)
        branch_addresses.append(branch_address)
        citys.append(city)
        states.append(state)
        zipcodes.append(zipcode)
        opening_dates.append(opening_date_millis)
        row_num += 1

    df = pd.DataFrame({
        'branch_id': branch_ids,
        'branch_name': branch_names,
        'branch_address': branch_addresses,
        'city': citys,
        'state': states,
        'zipcode': zipcodes,
        'opening_date_millis': opening_dates
    })

    df.to_csv(output_file, index=False)

    print(f'CSV file {output_file} with {num_rows} rows has been generated successfully!')


with DAG(
    'branch_dim_generator',
    default_args=default_args,
    description='Generate large branch dimension data in a CSV file',
    schedule="0 0 * * *",
    start_date=start_date,
    tags=['schema', 'branch']
) as dag:

    start = EmptyOperator(
        task_id = 'start_task'
    )
    end = EmptyOperator(
        task_id = 'end_task'
    )

    generate_branch_dimension_data = PythonOperator(
        task_id = 'generate_branch_dimension_data',
        python_callable=generate_branch_dim_data
    )

    start >> generate_branch_dimension_data >> end