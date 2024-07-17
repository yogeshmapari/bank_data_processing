
import os
import json
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
# Airflow DAG definition
default_args =  {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
file_path="/home/kali/Desktop/projects/git/bank_data_processing/dags/query.txt"
mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
conn = mysql_hook.get_conn()
cursor = conn.cursor()

# Function to execute the query and log the results


def run_query():
    with open(file_path, 'r') as file:
        q1=file.read().strip()
    print(q1)
    cursor.execute(q1)
    results = cursor.fetchall()
    print(results)
    for row in results:
        print(row)

dag = DAG(
    'dag_run_query',
    default_args=default_args,
    description='Load JSON files from landing area to MySQL and move to archive',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1), # Adjust the start date as needed
    tags=['example'],
)

# Python operator to execute the load_json_files_to_mysql function
run_query = PythonOperator(
    task_id='run_query',
    python_callable=run_query,
    provide_context=True,  # This provides the task context (e.g., execution date)
    dag=dag,
)


# Define task dependencies
run_query