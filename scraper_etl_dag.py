# Load neccessary Libraries and functions from the scraper.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from scraper import *

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,4,10),
    'email': ['airflow@example.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

# Define the DAG
dag = DAG(
    'scraper_dag',
    default_args=default_args,
    description='Iterate Shops, transform and add to SQL DB',
    schedule_interval=timedelta(days=1)
)

# Define the tasks
task1 = PythonOperator(
    task_id='iterate_all_shops',
    python_callable=IterateAllShops,
    dag=dag
)

task2 = PythonOperator(
    task_id='transform_data',
    python_callable=IterateAllCsv,
    dag=dag
)

task3 = PythonOperator(
    task_id='add_data_to_sql',
    python_callable=AddToSQL,
    dag=dag
)

# Bring the different tasks into the correct order
task1 >> task2 >> task3