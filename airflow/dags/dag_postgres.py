import sys
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Ensure the script can find the 'ingest_data' module
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ingest_data import ingest_callable

# Define environment variables for PostgreSQL connection
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

# Define the URL template and output file path using Airflow macros for dynamic dates
URL_TEMPLATE = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
OUTPUT_FILE_TEMPLATE = os.path.join(AIRFLOW_HOME, "output_{{ execution_date.strftime('%Y-%m') }}.csv.gz")
TABLE_NAME_TEMPLATE = "yellow_taxi_{{ execution_date.strftime('%Y_%m') }}"

# Define the DAG
local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="6 17 * * *",  # Every day at 17:06
    start_date=datetime(2024, 6, 7),
    catchup=False  # Do not perform backfill
)

with local_workflow:
    # Define the wget task to download the file
    wget_task = BashOperator(
        task_id="download",
        bash_command=f"wget {URL_TEMPLATE} -O {OUTPUT_FILE_TEMPLATE}"
    )

    # Define the ingestion task
    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs={
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'port': PG_PORT,
            'db': PG_DATABASE,
            'table_name': TABLE_NAME_TEMPLATE,
            'csv_file': OUTPUT_FILE_TEMPLATE
        }
    )

    # Set the task dependencies
    wget_task >> ingest_task
