list1=["customers",
"accounts",
"transactions",
"loans",
"cards",
"branches",
"employees",
"atms",
"investments",
"customer_support",
"fixed_deposits",
"recurring_deposits",
"online_banking",
"bill_payments",
"insurance",
"credit_scores",
"service_charges",
"cheques",
"savings_goals",
"mortgage_applications"]

# data=
for i in list1:
    with open(f'C:/Users/yogesh/OneDrive/Desktop/projects/git/bank_data_processing/dags/dag_raw_{i}.py','w') as f:
        f.writelines(f"""
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
from scripts.raw_load_{i} import create_tables,tuncate_table,insert_table
# Airflow DAG definition
default_args =  {{
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}}

dag = DAG(
    'raw_load_{i}',
    default_args=default_args,
    description='Load JSON files from landing area to MySQL and move to archive',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1), # Adjust the start date as needed
    tags=['example'],
)
mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
conn = mysql_hook.get_conn()
cursor = conn.cursor()
# Python operator to execute the load_json_files_to_mysql function
create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_tables,
    op_kwargs={{'cursor': cursor }},
    provide_context=True,  # This provides the task context (e.g., execution date)
    dag=dag,
)
tuncate_table = PythonOperator(
    task_id='tuncate_table',
    python_callable=tuncate_table,
    op_kwargs={{'cursor': cursor }},
    provide_context=True,  # This provides the task context (e.g., execution date)
    dag=dag,
)
insert_table = PythonOperator(
    task_id='insert_table',
    python_callable=insert_table,
    op_kwargs={{'cursor': cursor }},
    provide_context=True,  # This provides the task context (e.g., execution date)
    dag=dag,
)

# Define task dependencies
create_table >>tuncate_table>>insert_table
""")
        pass