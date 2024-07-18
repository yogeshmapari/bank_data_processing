from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pandas as pd
import numpy as np
import mysql.connector


def transform_and_load_data(mysql_conn_id, source_table, target_table):
    # Create MySQL hook
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    
    # Get MySQL connection
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    
    # Load data from source table into DataFrame
    df = pd.read_sql(f'SELECT * FROM {source_table}', con=connection)
    

    # Create table if it doesn't exist
    cursor.execute(f"""
                   
    CREATE TABLE IF NOT EXISTS {target_table} (
        {', '.join([f'{col} {dtype}' for col, dtype in zip(df.columns, df.dtypes.replace({'object': 'VARCHAR(255)', 'int64': 'INT', 'float64': 'FLOAT', 'datetime64[ns]': 'DATETIME'}))])}
    )
    """)
    
    # Insert data into target table
    for _, row in df.iterrows():
        cursor.execute(f"""
        INSERT INTO {target_table} ({', '.join(df.columns)})
        VALUES ({', '.join(['%s'] * len(df.columns))})
        """, tuple(row))
    
    # Commit changes
    connection.commit()
    cursor.close()
    connection.close()

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the list of source and target table pairs
table_pairs = [
('dlt_accounts','int_accounts'),
('dlt_atms','int_atms'),
('dlt_bill_payments','int_bill_payments'),
('dlt_branches','int_branches'),
('dlt_cards','int_cards'),
('dlt_cheques','int_cheques'),
('dlt_credit_scores','int_credit_scores'),
('dlt_customer_support','int_customer_support'),
('dlt_customers','int_customers'),
('dlt_employees','int_employees'),
('dlt_fixed_deposits','int_fixed_deposits'),
('dlt_insurance','int_insurance'),
('dlt_investments','int_investments'),
('dlt_loans','int_loans'),
('dlt_mortgage_applications','int_mortgage_applications'),
('dlt_online_banking','int_online_banking'),
('dlt_recurring_deposits','int_recurring_deposits'),
('dlt_savings_goals','int_savings_goals'),
('dlt_service_charges','int_service_charges'),
('dlt_transactions','int_transactions')
    # Add more table pairs as needed
]

# Define the DAG
with DAG(
    'dag_incremental_script',
    default_args=default_args,
    description='A DAG to transform data from MySQL tables and load into other tables',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # # Dynamically create tasks for each table pair
    # for source_table, target_table in table_pairs:
    #     task = PythonOperator(
    #         task_id=f'transform_and_load_{source_table}_to_{target_table}',
    #         python_callable=transform_and_load_data,
    #         op_kwargs={
    #             'mysql_conn_id': 'mysql_default',  # Replace with your MySQL connection ID in Airflow
    #             'source_table': source_table,
    #             'target_table': target_table
    #         },
    #     )
    
    previous_task = None  # To keep track of the last task created
    
    # Dynamically create tasks for each table pair
    for target_table,source_table in table_pairs:
        task = PythonOperator(
            task_id=f'transform_and_load_{source_table}_to_{target_table}',
            python_callable=transform_and_load_data,
            op_kwargs={
                'mysql_conn_id': 'mysql_default',  # Replace with your MySQL connection ID in Airflow
                'source_table': source_table,
                'target_table': target_table
            },
        )
        
        # Set linear dependencies
        if previous_task:
            previous_task >> task  # Set the previous task to run before the current task
        
        previous_task = task  # Update previous_task to the current task