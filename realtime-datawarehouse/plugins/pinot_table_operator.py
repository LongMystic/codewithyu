import glob
import requests
from airflow.models import BaseOperator
from airflow.utils.context import Context
from typing import Any

class PinotTableSubmitOperator(BaseOperator):

    def __init__(
        self,
        folder_path,
        pinot_url,
        *args,
        **kwargs
    ):
        super(PinotTableSubmitOperator, self).__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.pinot_url = pinot_url


    def execute(self, context: Context) -> Any:
        try:
            table_files = glob.glob(self.folder_path + '/*.json')
            for table_file in table_files:
                with open(table_file, 'r') as f:
                    table_data = f.read()

                    # define the headers and submit the post request to pinot
                    headers = {
                        'Content-Type': 'application/json'
                    }
                    response = requests.post(self.pinot_url, headers=headers, data=table_data)

                    if response.status_code == 200:
                        self.log.info(f"Table successfully submitted to Apache Pinot! {table_data}")
                    elif response.status_code == 409:
                        print("Table already exists, skipping")
                    else:
                        self.log.error(f'Failed to submit Table: {response.status_code} - {response.text}')
                        raise Exception(f'Table submission failed with status code {response.status_code}')

        except Exception as e:
            self.log.error(f'An error occured: {str(e)}')