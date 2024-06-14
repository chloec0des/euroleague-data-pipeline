from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Add the base directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from lib.euroleague.fetch_euro_data import fetch_euro_data_from_api
from lib.euroleague.load_euroleague_data_to_bigquery import load_euroleague_data_to_bigquery
from lib.euroleague.raw_to_formatted_euroleague import convert_raw_to_formatted_euroleague
from dags.lib.euroleague.utils import create_directories
from lib.euroleague.load_euroleague_data_to_elasticsearch import load_euroleague_data_to_elasticsearch

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

with DAG('euroleague_data_pipeline', default_args=default_args, schedule='@daily') as dag:
    create_dirs_task = PythonOperator(task_id='create_directories', python_callable=create_directories)
    fetch_task = PythonOperator(task_id='fetch_euro_data', python_callable=fetch_euro_data_from_api)
    convert_task = PythonOperator(task_id='convert_raw_to_formatted_euroleague', python_callable=convert_raw_to_formatted_euroleague)
    load_bigquery_task = PythonOperator(task_id='load_euroleague_data_to_bigquery', python_callable=load_euroleague_data_to_bigquery)
    load_elasticsearch_task = PythonOperator(task_id='load_euroleague_data_to_elasticsearch', python_callable=load_euroleague_data_to_elasticsearch)

    create_dirs_task >> fetch_task >> convert_task >> load_bigquery_task >> load_elasticsearch_task
